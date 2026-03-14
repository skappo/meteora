#!/usr/bin/env python3
"""
meteora_pipeline_pi5_v1.py

Proof-of-Concept: multi-thread/multiprocess pipeline for continuous night-sky capture,
detection of meteors, 30s event recording via hardware H.264 (ffmpeg), and timelapse stacking.

Designed for Raspberry Pi 5 (but works on other Linux SBCs).

Features:
- capture thread (supports Picamera2 or OpenCV VideoCapture)
- detection worker (frame-differencing + simple morphology) using multiprocessing
- event recorder (saves 30s H264 video via ffmpeg hardware encoder)
- timelapse worker (accumulates N frames, aligns with ECC, saves average stack)
- JSON metadata logging per event/timelapse
- rotating logs and safe shutdown

Usage:
- install dependencies: pip3 install opencv-python-headless numpy psutil pyexiv2
  (note: on Raspberry Pi prefer opencv built with V4L2 and libcamera; picamera2 optional)
- There is no hardware H264 or H265 encoder on Pi5. So use software libx264
- run: python3 meteora_pipeline_pi5.py --config config.yaml

DEBUG
    If debug_level = 1 (Low Overhead):
        You get a live JSON stream over ZMQ telling you noise levels, threshold, and exactly how many contours were rejected by size.
        You get specific alignment metrics (Correlation, Shift distance) in the logs.
    If debug_level = 2 (Diagnosis):
        If alignment is failing, you get a "Mosaic" image on disk showing exactly why (is it noise? is it clouds?).
        During an event, you get snapshots of the detection pipeline saved to disk.
    If debug_level = 3 (Full Stream):
        You get live video of the quad-view (original/diff/thresh/contours) over the ZMQ socket for remote viewing.
    If debug_level = 4 (Variable Stream):
        You get a list of almost all self variable excluing all images and hardware objects
        
### 📊 Resolution-Dependent Tuning Table

| Feature Variable | **1280 × 720** (HD) | **1920 × 1080** (FHD) | **2304 × 1296** (2K) | **4056 × 3040** (12MP) | **Why this changes** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `min_area` | **50** | **150** | **250** | **600** | Meteors are larger streaks in high res. |
| `max_queue_size` | **1000** | **400** | **300** | **50** | RAM Safety. High res frames consume massive RAM. |
| `base_threshold` | **15 - 20** | **20 - 25** | **25 - 30** | **35 - 45** | Sensor noise is "sharper" in high resolution. |
| `min_changes`* | **20** | **40** | **60** | **150** | Prevents system from analyzing tiny noise patterns. |
| `mask radius` | **5** | **10** | **12** | **20** | Stars cover more area; search cone must widen. |
| `max_event_frames` | **300** | **150** | **100** | **30** | RAM bottleneck. Too many high-res frames crash Python. |
| `JPEG Quality` | **85** | **75** | **70** | **60** | Balance between clarity and SFTP upload time. |

"""

import argparse
import json
import logging
import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import cv2
import glob 
import numpy as np
import psutil
import io
import paramiko
import ntplib
import zmq      #  for the debug viewer
import csv
import html
import requests   
import gpiod
import random          
from zipfile import ZipFile
from enum import Enum, auto
from collections import deque
from datetime import datetime, timezone, time as dtime
from logging.handlers import RotatingFileHandler
from picamera2 import Picamera2
from pydantic import BaseModel, Field, conint, confloat, constr, field_validator, model_validator, ValidationError
from typing import Tuple, Optional, List

def init_gpio():
    for chip_path in ['/dev/gpiochip4', '/dev/gpiochip0']:
        if os.path.exists(chip_path):
            return gpiod.Chip(chip_path), chip_path
    raise FileNotFoundError("No known Raspberry Pi GPIO chip found")

try:
    GPIO_CHIP, chip_path = init_gpio()
    IS_GPIO_AVAILABLE = True
    logging.info(f"GPIO enabled on {chip_path}")
except (ImportError, FileNotFoundError, OSError) as e:
    IS_GPIO_AVAILABLE = False
    GPIO_CHIP = None
    logging.warning(f"GPIO support is DISABLED (Reason: {e}). Power monitor and LED will not run.")
    
# --------------------------- Configuration ---------------------------
DEFAULTS = {
    "capture": {
        "width": 1280,
        "height": 720,
        "exposure_us": 200000,
        "gain": 8.0,
        "red_gain": 2.0,
        "blue_gain": 1.5,
        "auto_exposure_tuning": False,
        "target_sky_brightness": 15,  # Target median brightness on a 0-255 scale
        "min_exposure_us": 100000,    # 0.1 seconds
        "max_exposure_us": 800000,    # 0.8 seconds (CRUCIAL meteor constraint)
        "snap_exposure_us": 8000000,  # 8 sec
        "snap_gain": 4,
        "min_gain": 1.0,
        "max_gain": 12.0,             # The practical maximum before excessive noise  
        "tolerance": 2,
        "max_attempts": 3,
        "max_brightness": 240
    },
    "detection": {
        "min_area": 50,
        "pre_event_frames": 8,
        "event_cooldown_frames": 16,
        "max_event_frames": 300,
        "dark_frame_path": None,
        "bias_frame_path": None,
        "flat_frame_path": None,
        "auto_sensitivity_tuning": False,
        "base_threshold": 15,          # <------
        "noise_factor": 1.2,              # <------
        "noise_smoothing_alpha": 0.05,
        "static_threshold": 25,
        "min_threshold": 10,
        "max_threshold": 100
    },
    "timelapse": {
        "stack_N": 20,
        "stack_align": True,
        "astro_stretch": True,
        "astro_stretch_strength": 100,
        "nightmode_interval_sec": 0,
        "daymode_interval_sec": 60,
        "stack_method": "mean",
        "min_features_for_alignment": 3,
        "auto_stack_method": False,
        "auto_stack_brightness_threshold": 40,
        "auto_sum_darkness_threshold": 15,
        "sum_brightness_scale": 0.3
    },
    "timelapse_video": {
        "enabled": True,
        "video_fps": 10,
        "ffmpeg_encoder": "libx264",
        "ffmpeg_bitrate": "2000k"
    },
    "events": {
        "video_fps": 4,
        "jpeg_quality": 85
    },
    "monitor": {
        "interval_sec": 5
    },
    "power_monitor": {
        "enabled": True,
        "pin": 17,
        "shutdown_delay_sec": 60
    },
    "led_status": {
        "enabled": True,
        "pin": 26
    },
    "daylight": {
        "enabled": True,
        "stddev_threshold": 10.0,   # <----
        "min_stars": 15,
        "check_interval_min": 30,
        "lux_threshold": 20.0
    },
    "janitor": {
        "monitor_path": "/",
        "log_rotation_mb": 10,
        "log_backup_count": 5,
        "csv_rotation_mb": 20,
        "csv_backup_count": 5,
        "threshold": 90.0,
        "target": 75.0,
        "priority_delete": [
            "daylight",   # First, delete old sky condition snapshots
            "timelapse",  # Next, delete old timelapses
            "events"      # Last resort, delete old events      
        ]                      
    },
    "dashboard": {
        "enabled": True,
        "host": "0.0.0.0",
        "port": 5000
    },
    "heartbeat": {
        "enabled": True,
        "url": None,
        "interval_min": 60
    },
    "event_log": {
        "enabled": True
    },
    "health_monitor": {
        "log_rotation_mb": 5,
        "log_backup_count": 3
    },
    "sftp": {
        "enabled": False,
        "host": "sftp.myserver.net",
        "port": 22,
        "user": "myuser",
        "remote_dir": "/remote/path",
        "max_queue_size": 500
    },
    "ntp": {
        "enabled": True,
        "server": "pool.ntp.org",
        "sync_interval_hours": 6,
        "max_offset_sec": 2.0
    },
    "general": {
        "version": "197.17",
        "debug_level": 0,
        "debug_visualization": False,
        "record_raw_frames": False,
        "log_dir": "logs",
        "max_queue_size": 1000,
        "hostname": socket.gethostname(),
        "location": None,
        "latitude": 0.0,
        "longitude": 0.0,
        "max_camera_failures": 20,
        "max_restart_failures": 3,
        "idle_heartbeat_interval_min": 5,
        "power_monitor_pin": 17,
        "camera_azimuth": 0.0,    # 0=North, 90=East, 180=South, 270=West
        "camera_altitude": 90.0,  # 90=Zenith (up), 0=Horizon
        "lens_hfov": 80.0,        # Horizontal Field of View in degrees
        "lens_vfov": 50.0,        # Vertical Field of View in degrees
        "maintenance_timeout": 300,  
        "shutdown_time": None,
        "start_time": "15:00",
        "end_time": "06:00"
    }
}
# Values tuned for optimal balance of sensitivity and RAM safety
RESOLUTION_TUNING_PROFILES = {
    # format: TotalPixels (MP) : {settings}
    0.5: { "min_area": 30,  "min_changes": 10,  "max_q": 1000, "radius": 4,  "std_floor": 2.5 }, # VGA
    1.0: { "min_area": 60,  "min_changes": 22,  "max_q": 1000, "radius": 5,  "std_floor": 2.0 }, # 720p
    2.0: { "min_area": 180, "min_changes": 50,  "max_q": 350,  "radius": 10, "std_floor": 1.0 }, # 1080p
    3.0: { "min_area": 300, "min_changes": 80,  "max_q": 250,  "radius": 12, "std_floor": 0.8 }, # 2K
    12.3:{"min_area": 700, "min_changes": 300, "max_q": 60,   "radius": 20, "std_floor": 0.5 }  # 4K / HQ
}
# --------------------------- Configuration Schema (Pydantic) ---------------------------
class CaptureConfig(BaseModel):
    width: conint(gt=0,le=8000) = DEFAULTS["capture"]["width"]
    height: conint(gt=0,le=8000) = DEFAULTS["capture"]["height"]
    exposure_us: conint(gt=0,le=10000000) = DEFAULTS["capture"]["exposure_us"]
    gain: confloat(ge=0,le=16) = DEFAULTS["capture"]["gain"]
    red_gain: confloat(gt=0,le=16) = DEFAULTS["capture"]["red_gain"]
    blue_gain: confloat(gt=0,le=16) = DEFAULTS["capture"]["blue_gain"]
    auto_exposure_tuning: bool = DEFAULTS["capture"]["auto_exposure_tuning"]
    target_sky_brightness: conint(ge=0, le=255) = DEFAULTS["capture"]["target_sky_brightness"]
    min_exposure_us: conint(gt=0) = DEFAULTS["capture"]["min_exposure_us"]
    max_exposure_us: conint(gt=0) = DEFAULTS["capture"]["max_exposure_us"]
    snap_exposure_us: conint(gt=0) = DEFAULTS["capture"]["snap_exposure_us"]
    snap_gain: confloat(gt=0,le=16) = DEFAULTS["capture"]["snap_gain"]
    min_gain: confloat(ge=1.0) = DEFAULTS["capture"]["min_gain"]
    max_gain: confloat(ge=1.0, le=16.0) = DEFAULTS["capture"]["max_gain"]      
    tolerance: conint(gt=0,le=10) = DEFAULTS["capture"]["tolerance"]
    max_attempts: conint(gt=0,le=10) = DEFAULTS["capture"]["max_attempts"]
    max_brightness: conint(gt=0,le=240) = DEFAULTS["capture"]["max_brightness"]
    @model_validator(mode='after')
    def validate_logical_bounds(self):
        """
        Enforces logical consistency between min/max and target limits.
        """
        # 1. Exposure Range
        if self.min_exposure_us >= self.max_exposure_us:
            raise ValueError(
                f"Configuration Error: 'min_exposure_us' ({self.min_exposure_us}) "
                f"must be strictly less than 'max_exposure_us' ({self.max_exposure_us})."
            )

        # 2. Gain Range
        if self.min_gain >= self.max_gain:
            raise ValueError(
                f"Configuration Error: 'min_gain' ({self.min_gain}) "
                f"must be strictly less than 'max_gain' ({self.max_gain})."
            )

        # 3. Brightness Targets (Target vs Saturation Limit)
        if self.target_sky_brightness >= self.max_brightness:
            raise ValueError(
                f"Configuration Error: 'target_sky_brightness' ({self.target_sky_brightness}) "
                f"must be less than the saturation limit 'max_brightness' ({self.max_brightness})."
            )
            
        return self

class DetectionConfig(BaseModel):
    min_area: conint(gt=0,le=255) = DEFAULTS["detection"]["min_area"]
#    accumulate_alpha: confloat(gt=0, lt=1) = DEFAULTS["detection"]["accumulate_alpha"]
    pre_event_frames: conint(ge=0, le=100) = DEFAULTS["detection"]["pre_event_frames"]
    event_cooldown_frames: conint(ge=0) = DEFAULTS["detection"]["event_cooldown_frames"]
    max_event_frames: conint(ge=0) = DEFAULTS["detection"]["max_event_frames"]
    dark_frame_path: Optional[str] = DEFAULTS["detection"]["dark_frame_path"]
    bias_frame_path: Optional[str] = DEFAULTS["detection"]["bias_frame_path"] 
    flat_frame_path: Optional[str] = DEFAULTS["detection"]["flat_frame_path"]
    auto_sensitivity_tuning: bool = DEFAULTS["detection"]["auto_sensitivity_tuning"] 
    base_threshold: conint(ge=0, le=255) = DEFAULTS["detection"]["base_threshold"]
    noise_factor: confloat(ge=0.0) = DEFAULTS["detection"]["noise_factor"]   
    noise_smoothing_alpha: confloat(ge=0.0) = DEFAULTS["detection"]["noise_smoothing_alpha"]
    static_threshold: conint(ge=0, le=255) = DEFAULTS["detection"]["static_threshold"]  
    min_threshold: conint(ge=0, le=255) = DEFAULTS["detection"]["min_threshold"]
    max_threshold: conint(ge=0, le=255) = DEFAULTS["detection"]["max_threshold"]   
    @field_validator('dark_frame_path', mode='before')
    def allow_empty_str_for_optional_path(cls, v):
        # If the input is an empty string, convert it to None before other validation.
        if v == '':
            return None
        return v

class TimelapseConfig(BaseModel):
    stack_N: conint(gt=0,le=100) = DEFAULTS["timelapse"]["stack_N"]
    stack_align: bool = DEFAULTS["timelapse"]["stack_align"]
    astro_stretch: bool = DEFAULTS["timelapse"]["astro_stretch"]
    astro_stretch_strength: conint(ge=0, le=100) = DEFAULTS["timelapse"]["astro_stretch_strength"]
    nightmode_interval_sec: int = DEFAULTS["timelapse"]["nightmode_interval_sec"]
    daymode_interval_sec: int = DEFAULTS["timelapse"]["daymode_interval_sec"]
    stack_method: str = DEFAULTS["timelapse"]["stack_method"]
    min_features_for_alignment: conint(ge=1, le=10) = DEFAULTS["timelapse"]["min_features_for_alignment"]
    auto_stack_method: bool = DEFAULTS["timelapse"]["auto_stack_method"]
    auto_stack_brightness_threshold: conint(ge=0, le=255) = DEFAULTS["timelapse"]["auto_stack_brightness_threshold"]
    auto_sum_darkness_threshold: conint(ge=0, le=255) = DEFAULTS["timelapse"]["auto_sum_darkness_threshold"]
    sum_brightness_scale: confloat(ge=0.0, le=1.0) = DEFAULTS["timelapse"]["sum_brightness_scale"]
    @field_validator('*', mode='after')
    def validate_thresholds(cls, v, values):
        # This validator runs after all individual fields are populated.
        # We only need to run our logic once, so we check for the presence of both keys.
        if 'auto_sum_darkness_threshold' in values.data and 'auto_stack_brightness_threshold' in values.data:
            dark_thresh = values.data['auto_sum_darkness_threshold']
            bright_thresh = values.data['auto_stack_brightness_threshold']

            if dark_thresh >= bright_thresh:
                raise ValueError(
                    f"Configuration Error in [timelapse]: auto_sum_darkness_threshold ({dark_thresh}) "
                    f"must be less than auto_stack_brightness_threshold ({bright_thresh})."
                )
        return v    

class TimelapseVideoConfig(BaseModel):
    enabled: bool = DEFAULTS["timelapse_video"]["enabled"]
    video_fps: conint(gt=0,le=30) = DEFAULTS["timelapse_video"]["video_fps"]
    ffmpeg_encoder: str = DEFAULTS["timelapse_video"]["ffmpeg_encoder"]
    ffmpeg_bitrate: str = DEFAULTS["timelapse_video"]["ffmpeg_bitrate"]

class EventsConfig(BaseModel):
    video_fps: conint(gt=0,le=30) = DEFAULTS["events"]["video_fps"]
    jpeg_quality: conint(ge=1, le=100) = DEFAULTS["events"]["jpeg_quality"]

class MonitorConfig(BaseModel):
    interval_sec: conint(gt=0,le=60) = DEFAULTS["monitor"]["interval_sec"]

class PowerMonitorConfig(BaseModel):
    enabled: bool = DEFAULTS["power_monitor"]["enabled"]
    pin: int = DEFAULTS["power_monitor"]["pin"]
    shutdown_delay_sec: conint(ge=0) = DEFAULTS["power_monitor"]["shutdown_delay_sec"]

class LedStatusConfig(BaseModel):
    enabled: bool = DEFAULTS["led_status"]["enabled"]
    pin: int = DEFAULTS["led_status"]["pin"]

class DaylightConfig(BaseModel):
    enabled: bool = DEFAULTS["daylight"]["enabled"]
    stddev_threshold: float = DEFAULTS["daylight"]["stddev_threshold"]
    min_stars: conint(gt=10,le=50) = DEFAULTS["daylight"]["min_stars"]
    check_interval_min: conint(ge=1,le=60) = DEFAULTS["daylight"]["check_interval_min"]
    lux_threshold: float = DEFAULTS["daylight"]["lux_threshold"]

class JanitorConfig(BaseModel):
    monitor_path: str = DEFAULTS["janitor"]["monitor_path"]
    log_rotation_mb: conint(gt=5,le=20) = DEFAULTS["janitor"]["log_rotation_mb"]
    log_backup_count: conint(gt=0,le=10) = DEFAULTS["janitor"]["log_backup_count"]
    csv_rotation_mb: conint(gt=5,le=20) = DEFAULTS["janitor"]["csv_rotation_mb"]
    csv_backup_count: conint(gt=0,le=10) = DEFAULTS["janitor"]["csv_backup_count"]
    threshold: float = DEFAULTS["janitor"]["threshold"]
    target: float = DEFAULTS["janitor"]["target"]
    priority_delete: List[str] = DEFAULTS["janitor"]["priority_delete"]
    @model_validator(mode='after')
    def validate_disk_logic(self):
        # Ensures that the cleanup trigger threshold is higher than the target cleanup level.
        if self.threshold <= self.target:
            raise ValueError(
                f"Configuration Error: Janitor 'threshold' ({self.threshold}%) "
                f"must be greater than 'target' ({self.target}%)."
            )
        return self
    
class DashboardConfig(BaseModel):
    enabled: bool = DEFAULTS["dashboard"]["enabled"]
    host: str = DEFAULTS["dashboard"]["host"]
    port: conint(gt=1023, lt=65536) = DEFAULTS["dashboard"]["port"]

class HeartbeatConfig(BaseModel):
    enabled: bool = DEFAULTS["heartbeat"]["enabled"]
    url: Optional[str] = DEFAULTS["heartbeat"]["url"]
    interval_min: conint(gt=10,le=60) = DEFAULTS["heartbeat"]["interval_min"]

class EventLogConfig(BaseModel):
    enabled: bool = DEFAULTS["event_log"]["enabled"]

class HealthMonitorConfig(BaseModel):
    log_rotation_mb: conint(gt=0) = DEFAULTS["health_monitor"]["log_rotation_mb"]
    log_backup_count: conint(gt=0) = DEFAULTS["health_monitor"]["log_backup_count"]

class SftpConfig(BaseModel):
    enabled: bool = DEFAULTS["sftp"]["enabled"]
    host: str = DEFAULTS["sftp"]["host"]
    port: int = DEFAULTS["sftp"]["port"]
    user: str = DEFAULTS["sftp"]["user"]
    remote_dir: str = DEFAULTS["sftp"]["remote_dir"]
    max_queue_size: int = DEFAULTS["sftp"]["max_queue_size"]

class NtpConfig(BaseModel):
    enabled: bool = DEFAULTS["ntp"]["enabled"]
    server: str = DEFAULTS["ntp"]["server"]
    sync_interval_hours: conint(ge=0) = DEFAULTS["ntp"]["sync_interval_hours"]
    max_offset_sec: float = DEFAULTS["ntp"]["max_offset_sec"]

class GeneralConfig(BaseModel):
    version: str = DEFAULTS["general"]["version"]
    debug_visualization : bool = DEFAULTS["general"]["debug_visualization"]
    debug_level: int = Field(default=0, ge=0, le=4, description="0=Off, 1=Stats, 2=Images, 3=Full/Stream, 4=Variables dump")
    record_raw_frames: bool = Field(default=False, description="Save raw frames for replay")
    log_dir: str = DEFAULTS["general"]["log_dir"]
    max_queue_size: conint(gt=0,le=1000) = DEFAULTS["general"]["max_queue_size"]
    hostname: str = DEFAULTS["general"]["hostname"]
    location: Optional[str] = DEFAULTS["general"]["location"]
    latitude: float = DEFAULTS["general"]["latitude"]
    longitude: float = DEFAULTS["general"]["longitude"]
    max_camera_failures: conint(gt=0) = DEFAULTS["general"]["max_camera_failures"]
    max_restart_failures: conint(gt=0,le=50) = DEFAULTS["general"]["max_restart_failures"]
    idle_heartbeat_interval_min: conint(gt=0) = DEFAULTS["general"]["idle_heartbeat_interval_min"]
    power_monitor_pin: int = DEFAULTS["general"]["power_monitor_pin"]
    camera_azimuth : confloat(ge=0.0,le=360.0) = DEFAULTS["general"]["camera_azimuth"]
    camera_altitude : confloat(ge=0.0,le=90.0) = DEFAULTS["general"]["camera_altitude"]
    lens_hfov : conint(ge=0,le=360) = DEFAULTS["general"]["lens_hfov"]
    lens_vfov : conint(ge=0,le=360) = DEFAULTS["general"]["lens_vfov"]  
    maintenance_timeout: conint(gt=0) = DEFAULTS["general"]["maintenance_timeout"]
    shutdown_time: Optional[constr(pattern=r'^\d{2}:\d{2}$')] = DEFAULTS["general"]["shutdown_time"]
    start_time: constr(pattern=r'^\d{2}:\d{2}$') = DEFAULTS["general"]["start_time"]
    end_time: constr(pattern=r'^\d{2}:\d{2}$') = DEFAULTS["general"]["end_time"]
    @field_validator('location', 'shutdown_time', mode='before')
    def allow_empty_str_for_optionals(cls, v):
        # This single validator handles both 'location' and 'shutdown_time'
        if v == '':
            return None
        return v

class MainConfig(BaseModel):
    capture: CaptureConfig = Field(default_factory=CaptureConfig)
    detection: DetectionConfig = Field(default_factory=DetectionConfig)
    timelapse: TimelapseConfig = Field(default_factory=TimelapseConfig)
    timelapse_video: TimelapseVideoConfig = Field(default_factory=TimelapseVideoConfig)
    events: EventsConfig = Field(default_factory=EventsConfig)
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    power_monitor: PowerMonitorConfig = Field(default_factory=PowerMonitorConfig)
    led_status: LedStatusConfig = Field(default_factory=LedStatusConfig)
    daylight: DaylightConfig = Field(default_factory=DaylightConfig)
    janitor: JanitorConfig = Field(default_factory=JanitorConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    heartbeat: HeartbeatConfig = Field(default_factory=HeartbeatConfig)
    event_log: EventLogConfig = Field(default_factory=EventLogConfig)
    health_monitor: HealthMonitorConfig = Field(default_factory=HealthMonitorConfig)
    sftp: SftpConfig = Field(default_factory=SftpConfig)
    ntp: NtpConfig = Field(default_factory=NtpConfig)
    general: GeneralConfig = Field(default_factory=GeneralConfig) 
    @field_validator('*', mode='after')
    def check_stack_n_memory_usage(cls, v, values):
        # After all individual fields are validated, 
        # check if the configured stack_N value is safe for the system's available memory.
        # This validator runs for every field, but we only need to execute
        # our logic once all the necessary data is available.
        # We check if 'capture' and 'timelapse' are present in the 'values.data' dict.
        if 'capture' in values.data and 'timelapse' in values.data:
            capture_cfg = values.data['capture']
            timelapse_cfg = values.data['timelapse']
            
            # 1. Estimate memory per frame (width * height * 3 bytes/pixel)
            bytes_per_frame = capture_cfg.width * capture_cfg.height * 3
            
            # 2. Get available system memory
            try:
                # Use a fresh check to get current available memory
                available_mem_bytes = psutil.virtual_memory().available
            except Exception:
                # If psutil fails, fallback to a safe, low default (e.g., 2GB)
                available_mem_bytes = 2 * 1024 * 1024 * 1024
            
            # 3. Calculate a safe maximum stack size
            # We'll use a conservative limit: don't let the image buffer
            # consume more than 25% of the *available* RAM.
            safe_mem_for_stack = available_mem_bytes * 0.25
            safe_max_stack_n = int(safe_mem_for_stack / bytes_per_frame)
            
            # Failsafe: ensure the max is at least a reasonable number
            safe_max_stack_n = max(5, safe_max_stack_n)

            # 4. Perform the validation check
            if timelapse_cfg.stack_N > safe_max_stack_n:
                mem_per_frame_mb = bytes_per_frame / (1024*1024)
                configured_usage_mb = timelapse_cfg.stack_N * mem_per_frame_mb
                available_mem_mb = available_mem_bytes / (1024*1024)
                
                error_msg = (
                    f"Configuration failed: timelapse.stack_N ({timelapse_cfg.stack_N}) is too high for the available system RAM. "
                    f"Estimated usage: {configured_usage_mb:.0f} MB. "
                    f"Available RAM: {available_mem_mb:.0f} MB. "
                    f"Recommended maximum stack_N for this resolution is ~{safe_max_stack_n}."
                )
                raise ValueError(error_msg)
        
        return v # Must return the value for the next validator
    # --------------------------- Logging ---------------------------
class ThreadColorLogFormatter(logging.Formatter):
    """
    A custom logging formatter that assigns a unique color to each thread,
    in addition to coloring the log level.
    """
    RESET = "\033[0m"
    
    # A palette of high-contrast colors for thread names
    THREAD_COLORS = [
        # Standard
        "\033[36m",    # Cyan
        "\033[33m",    # Yellow
        "\033[35m",    # Magenta
        "\033[32m",    # Green
        "\033[34m",    # Blue
        "\033[31m",    # Red
        # Bright
        "\033[96m",    # Bright Cyan
        "\033[93m",    # Bright Yellow
        "\033[95m",    # Bright Magenta
        "\033[92m",    # Bright Green
        "\033[94m",    # Bright Blue
        "\033[91m",    # Bright Red
        # Bold
        "\033[36;1m",  # Bold Cyan
        "\033[33;1m",  # Bold Yellow
        "\033[35;1m",  # Bold Magenta
        "\033[32;1m",  # Bold Green
        "\033[34;1m",  # Bold Blue
    ]

    LEVEL_COLORS = {
        logging.DEBUG: "\033[90m",      # Grey
        logging.INFO: "\033[37m",       # White
        logging.WARNING: "\033[93;1m",  # Bright Yellow Bold
        logging.ERROR: "\033[91;1m",    # Bright Red Bold
        logging.CRITICAL: "\033[97;41m", # White on Red Background
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread_color_map = {}
        self.lock = threading.Lock()

    def _get_thread_color(self, thread_name):
        # Assigns a persistent color to a thread name if not already assigned.
        with self.lock:
            if thread_name not in self.thread_color_map:
                new_color = self.THREAD_COLORS[len(self.thread_color_map) % len(self.THREAD_COLORS)]
                self.thread_color_map[thread_name] = new_color
            return self.thread_color_map[thread_name]

    def format(self, record):
        # Formats the log record with level and thread colors.
        level_color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        thread_color = self._get_thread_color(record.threadName)

        # Create the formatted string with embedded ANSI color codes
        log_line = (
            f"\033[90m{self.formatTime(record, self.datefmt)}{self.RESET} "
            f"{level_color}[{record.levelname}]{self.RESET} "
            f"{thread_color}[{record.threadName}]{self.RESET} "
            f"{record.getMessage()}"
        )
        
        return log_line
    # --------------------------- Camera capture using Picamera2 ---------------------------
class CameraCapture:
    def __init__(self, cfg):
        self.cfg = cfg
        self.picam2 = Picamera2()
        # raw_config = self.picam2.create_still_configuration(raw={"format": "SRGGB10", "size": (cfg['width'], cfg['height'])})
        main_config = self.picam2.create_still_configuration(main={"size": (cfg['width'], cfg['height']), "format": "RGB888"})
        self.picam2.configure(main_config)

#        if not cfg.get("auto_exposure", False):
        red_gain = cfg.get("red_gain", 2.0)
        blue_gain = cfg.get("blue_gain", 1.5)
        
        controls = {
            "ExposureTime": cfg.get("exposure_us", 200000),
            "AnalogueGain": cfg.get("gain", 8.0),
            "AwbEnable": False,  # Fixed white balance
            "ColourGains": [red_gain, blue_gain]
        }
        self.picam2.set_controls(controls)
#        else:
#            self.picam2.set_controls({"AeEnable": True})

        self.picam2.start()
        time.sleep(2)
        logging.info("CameraCapture initialized successfully.")
            
    def read(self):
        try:
            request = self.picam2.capture_request()
            try:
                img_array = request.make_array("main")
            finally:
                request.release()          # always released, even on exception
            if img_array  is None:
                return None
            bgr_8bit = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            return (bgr_8bit.astype(np.uint16) * 256)
        except Exception as e:
            logging.warning("Frame capture failed: %s", e)
            return None
            
    def flush(self, target_exposure_us=None, target_gain=None, max_attempts=15):
        """
        Dynamically flushes the exact number of stale frames from the hardware pipeline
        by inspecting the metadata of each frame until it matches the new settings.
        """
        if target_exposure_us is None:
            # Fallback to simple flush if no targets are provided
            for _ in range(4):
                try: self.picam2.capture_array()
                except Exception: pass
            return

        dropped = 0
        for _ in range(max_attempts):
            try:
                # capture_request is much faster/lighter than capture_array
                req = self.picam2.capture_request()
                meta = req.get_metadata()
                req.release()  # Critical: Free the memory buffer instantly

                actual_exp = meta.get("ExposureTime", 0)
                actual_gain = meta.get("AnalogueGain", 0.0)
                
                # --- NEW: WIDE TOLERANCE ---
                # Allow the hardware driver to round exposure by up to 5%
                allowed_exp_variance = target_exposure_us * 0.05

                # Check if this frame has our new settings
                exp_match = abs(actual_exp - target_exposure_us) < allowed_exp_variance  # 1ms tolerance
                gain_match = target_gain is None or abs(actual_gain - target_gain) < 0.5

                if exp_match and gain_match:
                    if dropped > 0:
                        logging.debug(f"Camera: Pipeline synced perfectly. Flushed exactly {dropped} stale frame(s).")
                    return # We reached the new frame! Stop flushing immediately.

                dropped += 1
            except Exception as e:
                logging.warning(f"Camera flush error: {e}")
                break
                
        logging.warning(f"Camera: Flush hit max attempts ({max_attempts}) without confirming sync.")

    def release(self):
        try:
            self.picam2.stop()
            logging.info("Camera stream stopped.")
            self.picam2.close()
            logging.info("Camera device closed and resources released.")
        except Exception as e:
            # Log an error but don't crash if the camera is already in a bad state
            logging.error(f"An error occurred during camera release: {e}")
    # --------------------------- Sky Simulator ---------------------------    
class FrameSimulator:
    """
    Ultra-Realistic Sky + Sensor Simulator (v4.0 Corrected)

    Optimisations:
    - Vectorised star rotation (NumPy)
    - Pre-allocated buffers
    - Pre-generated noise bank (60 frames)
    - Pre-computed star stamp masks (no per-star Python loop)

    Logging:
    - Day/Night transitions
    - Meteor / Satellite / Airplane events
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.width  = cfg.get("width",  1280)
        self.height = cfg.get("height", 720)
        self.frame_count = 0
        self.exposure_us = cfg.get("exposure_us", 800_000)

        # self.sim_params  = SimParams()           # FIX #14 — dataclass, not raw dict
        self.sim_params = {

            # Sky base
            "day_brightness_range":   (200, 220),
            "night_brightness_range": (20, 35),
            "day_noise_sigma":        2.0,
            "night_noise_sigma":      6.0,

            # Atmosphere multipliers
            "rayleigh_day_boost":     1.5,
            "rayleigh_night_rgb":     (0.2, 0.3, 0.8),

            # Sensor artefacts
            "hot_pixel_ratio":        0.000007,

            # Atmosphere / optics
            "light_pollution_grad":   2.0,
            "rayleigh_strength":      10.0,
            "mie_strength":           15.0,
            "vignette_strength":      0.3,
            "amp_glow_strength":      5.0,

            # Stars
            "star_count":             1200,
            "star_rotation_speed":    0.002,
            "pole_distance_from_center": 800.0,

            # Extinction
            "extinction_k":           0.3,

            # Moon
            "moon_radius_pixels":     40,
            "moon_glow_radius":       120,
            "moon_glow_strength":     0.4,
            "moon_earthshine_strength": 0.15,
            "moon_phase_override":    0.35,
            "moon_initial_offset_y":  400.0,
            "moon_arc_period":        10800,
            "moon_arc_peak_frac":     0.10,
            "moon_arc_horizon_frac":  0.90,

            # Clouds
            "cloud_layers": [
                {"speed": (0.6, 0.2),  "blobs": 8, "opacity": (0.1, 0.25), "blur": 201, "reseed": 18000},
                {"speed": (0.3, 0.1),  "blobs": 6, "opacity": (0.2, 0.45), "blur": 151, "reseed": 24000},
                {"speed": (0.1, 0.05), "blobs": 4, "opacity": (0.4, 0.70), "blur": 101, "reseed": 30000},
            ],
            "cloud_grey": (65.0, 60.0, 60.0),

            # Events
            "satellite_interval":     600,
            "airplane_interval":      900,
            "streak_duration":        20,

            # Meteors
            "meteor_interval":        300,
            "meteor_duration":        8,
            "meteor_trail_length":    20,
            "meteor_speed_min":       15.0,
            "meteor_speed_max":       35.0,
            "meteor_thickness_min":   2,
            "meteor_thickness_max":   5,
        }



        self.is_day_time = False
        self.ae_enabled  = False

        # FIX #1 — define `parent` BEFORE the inner class so the closure is valid
        parent = self

        class DummyControls:
            def set_controls(self_, controls):
                if "AeEnable" in controls:
                    parent.ae_enabled = controls["AeEnable"]
                    logging.info(f"SIM: Camera Control 'AeEnable' set to {parent.ae_enabled}")
                if "ExposureTime" in controls:
                    parent.exposure_us = controls["ExposureTime"]
                    logging.info(f"SIM: Camera Control 'ExposureTime' set to {parent.exposure_us}us")

            def capture_request(self_):
                return parent

            def get_metadata(self_):
                lux = 500.0 if parent.is_day_time else 0.05
                return {
                    "Lux": lux,
                    "ExposureTime": parent.exposure_us,
                    "AnalogueGain": 8.0,
                }

        self.picam2 = DummyControls()

        # Pre-allocated buffers
        self.frame_buffer = np.zeros((self.height, self.width, 3), dtype=np.float32)
        self.layer_buffer = np.zeros((self.height, self.width, 3), dtype=np.float32)

        self._init_static_maps()
        self._init_stars()
        self._init_clouds()
        self._init_streaks()
        self._init_meteors()
        self._init_noise_bank()      # FIX #8 — 60-frame bank
        self._init_fpn_hotpixels()

        self.moon_phase_data = self._get_moon_phase_fraction()

        logging.warning("=" * 60)
        logging.warning(f"=== SKY SIMULATOR v11.0 STARTED ({self.width}x{self.height}) ===")
        logging.warning(f"=== LOGGING: Enabled (Meteors/Satellites/Day-Night) ===")
        logging.warning("=" * 60)

    def _init_static_maps(self):
        p = self.sim_params
        h, w = self.height, self.width

        y_grad = np.linspace(1, 0, h, dtype=np.float32).reshape(-1, 1)
        x_grad = np.linspace(0, 1, w, dtype=np.float32)

        xx, yy = np.meshgrid(np.linspace(-1, 1, w), np.linspace(-1, 1, h))
        r2 = (xx ** 2 + yy ** 2).astype(np.float32)

        self.vignette_map    = (1 - p["vignette_strength"] * r2)[..., np.newaxis]
        glow                 = np.tile(x_grad ** 3, (h, 1))
        self.amp_glow_map    = (glow * p["amp_glow_strength"]).astype(np.float32)[..., np.newaxis]
        self.rayleigh_map    = ((y_grad ** 2) * p["rayleigh_strength"]).astype(np.float32)[..., np.newaxis]
        self.mie_map         = (np.exp(-(y_grad * 4)) * p["mie_strength"]).astype(np.float32)[..., np.newaxis]
        self.pollution_map   = np.linspace(0, p["light_pollution_grad"], h, dtype=np.float32).reshape(-1, 1)[..., np.newaxis]

        temp_dome = np.zeros((h, w), dtype=np.float32)
        cv2.circle(temp_dome, (w // 2, h + 100), w // 2, 1.0, -1)
        temp_dome = cv2.GaussianBlur(temp_dome, (0, 0), w * 0.05)
        self.city_dome_map = (temp_dome * 30.0).astype(np.float32)[..., np.newaxis]

    def _init_noise_bank(self):
        # FIX #8 — 60 frames eliminates visible cycling at normal frame rates
        bank_size = 60
        self.noise_bank = np.random.normal(
            0, 1.0, (bank_size, self.height, self.width, 3)
        ).astype(np.float32)
        self.noise_idx = 0

    def _init_stars(self):
        p = self.sim_params
        n = p["star_count"]
        max_dim = max(self.width, self.height) * 1.5

        self.stars_rel_pos = np.column_stack([
            np.random.uniform(-max_dim, max_dim, n),
            np.random.uniform(-max_dim, max_dim, n),
        ]).astype(np.float32)

        mag = np.random.pareto(2.0, n) + 1.0
        self.stars_brightness = np.clip(
            255 * (10 ** ((4.0 - mag) / 2.5)), 20, 255
        ).astype(np.float32)

        # FIX #2 — Renamed masks to reflect true RGB appearance
        #   White/neutral  : default (no mask)
        #   Warm yellow     : [1.0, 0.95, 0.7]   → reddish-yellow tint
        #   Cool red/orange : [1.0, 0.7, 0.5]    → orange-red
        #   Blue-white      : [0.8, 0.9, 1.0]    → blueish
        #   Deep blue       : [0.5, 0.6, 1.0]    → deep blue (O/B-type)
        colors = np.ones((n, 3), dtype=np.float32)
        r = np.random.rand(n)

        mask_warm_yellow = (r >= 0.65) & (r < 0.80)
        mask_orange_red  = (r >= 0.80) & (r < 0.90)
        mask_blue_white  = (r >= 0.90) & (r < 0.97)
        mask_deep_blue   = (r >= 0.97)

        # Colors in BGR order
        colors[mask_warm_yellow] = [0.70, 0.95, 1.0]   # warm yellow
        colors[mask_orange_red]  = [0.50, 0.70, 1.0]   # orange-red
        colors[mask_blue_white]  = [1.00, 0.90, 0.80]  # blue-white
        colors[mask_deep_blue]   = [1.00, 0.60, 0.50]  # deep blue

        self.stars_colors = colors * self.stars_brightness[:, np.newaxis]

        radii = np.random.choice([1, 2, 3], n, p=[0.70, 0.25, 0.05])
        self.star_rads = radii

        # FIX #6 — Pre-compute stamp masks; avoids per-star cv2.circle in render loop
        self.star_stamps: dict[int, np.ndarray] = {}
        for r_val in [2, 3]:
            stamp = cv2.getStructuringElement(
                cv2.MORPH_ELLIPSE, (2 * r_val + 1, 2 * r_val + 1)
            ).astype(np.float32)
            self.star_stamps[r_val] = stamp

    def _init_clouds(self):
        self.cloud_layers = []
        for params in self.sim_params["cloud_layers"]:
            layer = np.zeros((self.height, self.width), dtype=np.float32)
            self._generate_cloud_layer(layer, params)
            self.cloud_layers.append({
                "data":        layer,
                "opacity":     np.random.uniform(*params["opacity"]),
                "offset_x":   0.0,
                "offset_y":   0.0,
                "params":     params,
                "last_reseed": 0,
            })

    def _generate_cloud_layer(self, layer: np.ndarray, params: dict):
        """
        FIX #18 (v2) - True seamless tiling via 3x3 oversized canvas + center crop.

        Strategy:
          1. Paint blobs on a 3x3 grid of tiles (9 copies of the same pattern).
          2. Blur the whole oversized canvas — blur never touches the borders of
             the CENTER tile because it is surrounded by identical content.
          3. Crop only the center tile and resize to frame dimensions.
          4. np.roll can now scroll in any direction forever with zero seams.
        """
        h, w = self.height, self.width
        h4, w4 = h // 4, w // 4

        # Step 1: paint blobs onto a single tile
        single = np.zeros((h4, w4), dtype=np.uint8)
        for _ in range(params["blobs"]):
            cx   = np.random.randint(0, w4)
            cy   = np.random.randint(0, h4)
            size = np.random.randint(w4 // 8, w4 // 3)
            cv2.circle(single, (cx, cy), size, 255, -1)

        # Step 2: tile it 3x3 so blur has identical neighbours on every side
        tiled = np.tile(single, (3, 3))

        # Step 3: blur the whole 3x3 canvas
        k = params["blur"] // 4 * 2 + 1
        blurred = cv2.GaussianBlur(tiled, (k, k), 0)

        # Step 4: crop center tile — its edges are now seamless
        center = blurred[h4:h4*2, w4:w4*2]
        resized = cv2.resize(center, (w, h), interpolation=cv2.INTER_LINEAR)
        layer[:] = resized.astype(np.float32) / 255.0
        layer   *= np.random.uniform(*params["opacity"])

    def _init_streaks(self):
        self.streak_active    = False
        self.streak_type      = None
        self.streak_frame     = 0
        self.streak_pos       = np.array([0.0, 0.0])
        self.streak_vec       = np.array([0.0, 0.0])
        self.streak_color     = (255, 255, 255)
        self.streak_thickness = 1
        self.streak_dash      = False

    def _init_meteors(self):
        self.meteor_active = False
        self.meteor_frame  = 0
        self.meteor_trail  = []
        self.meteor_pos    = np.array([0.0, 0.0])
        self.meteor_vec    = np.array([0.0, 0.0])
        self.meteor_peak   = 255.0
        self.meteor_thick  = 2
        self.meteor_col    = (255, 255, 255)

    def _init_fpn_hotpixels(self):
        p = self.sim_params
        self.fpn = np.random.normal(0, 3.0, (self.height, self.width, 3)).astype(np.float32)
        hot_count = int(self.width * self.height * p["hot_pixel_ratio"])
        ys = np.random.randint(0, self.height, hot_count)
        xs = np.random.randint(0, self.width,  hot_count)
        self.hot_pixel_map = np.zeros((self.height, self.width, 3), dtype=np.float32)
        self.hot_pixel_map[ys, xs] = 200.0

    def _update_environment_state(self):
        new_state = 6 <= datetime.now().hour < 18
        if new_state != self.is_day_time:
            self.is_day_time = new_state
            label = "DAY (Blue Sky)" if new_state else "NIGHT (Stars Visible)"
            logging.info("SIM: ------------------------------------------------")
            logging.info(f"SIM: Environment Transition -> {label}")
            logging.info("SIM: ------------------------------------------------")

    def read(self) -> np.ndarray:
        start = time.time()
        self.frame_count += 1

        if self.frame_count % 300 == 0:
            mode = "Day" if self.is_day_time else "Night"
            logging.info(f"SIM: [Status] Frame {self.frame_count} | Mode: {mode}")

        self._update_environment_state()

        self._render_base_sky()
        self._render_noise()

        if not self.is_day_time:
            self._render_stars()
            self._render_moon()
            self._render_clouds()
            self._render_streaks()
            self._render_meteors()
            self.frame_buffer += self.hot_pixel_map

        self._render_vignette()

        # Output directly as BGR — no colour conversion needed
        frame_bgr = np.clip(self.frame_buffer, 0, 255).astype(np.uint8)

        # Respect simulated exposure time
        elapsed = time.time() - start
        delay = max(0.0, self.exposure_us / 1e6 - elapsed)
        if delay > 0.001:
            time.sleep(delay)

        return frame_bgr

    def _render_base_sky(self):
        """Fill frame with base sky brightness and atmospheric gradients."""
        p = self.sim_params
        if self.is_day_time:
            base_val = np.random.uniform(*p["day_brightness_range"])
            self.frame_buffer[:] = base_val
            # FIX #16 — rayleigh_day_boost is now a named param, not a magic 1.5
            self.frame_buffer[..., 0] += self.rayleigh_map[..., 0] * p["rayleigh_day_boost"]
        else:
            base_val = np.random.uniform(*p["night_brightness_range"])
            self.frame_buffer[:] = base_val
            self.frame_buffer += self.pollution_map
            # FIX #16 — rayleigh_night_rgb is now a named param
            self.frame_buffer += self.rayleigh_map * np.array(p["rayleigh_night_rgb"], dtype=np.float32)
            self.frame_buffer += self.mie_map
            self.frame_buffer += self.amp_glow_map
            self.frame_buffer += self.city_dome_map

    def _render_noise(self):
        """Add read noise (cycling bank) and fixed-pattern noise."""
        p = self.sim_params
        noise_sigma = p["day_noise_sigma"] if self.is_day_time else p["night_noise_sigma"]
        self.noise_idx = (self.noise_idx + 1) % len(self.noise_bank)
        self.frame_buffer += self.noise_bank[self.noise_idx] * noise_sigma
        self.frame_buffer += self.fpn

    def _render_stars(self):
        """
        Vectorised star rendering with:
         - airmass-based atmospheric extinction  (FIX #9)
         - altitude-dependent twinkle sigma      (FIX #13)
         - pre-computed stamp masks for r>1 stars (FIX #6)
         - direct star_rads indexing, no np.isin (FIX #7)
        """
        p = self.sim_params
        h, w = self.height, self.width
        theta = self.frame_count * p["star_rotation_speed"]
        c, s  = np.cos(theta), np.sin(theta)
        pole  = p["pole_distance_from_center"]

        rx = self.stars_rel_pos[:, 0]
        ry = self.stars_rel_pos[:, 1] + pole

        rot_x = rx * c - ry * s + (w / 2)
        rot_y = rx * s + ry * c + (h / 2) - pole

        valid_mask    = (rot_x >= 0) & (rot_x < w) & (rot_y >= 0) & (rot_y < h)
        valid_indices = np.where(valid_mask)[0]
        if len(valid_indices) == 0:
            return

        vx = rot_x[valid_indices].astype(np.int32)
        vy = rot_y[valid_indices].astype(np.int32)

        # FIX #9 — airmass-based extinction (replaces simple quadratic)
        zenith_norm = np.clip(vy / h, 0.0, 0.999)          # 0 = zenith, ~1 = horizon
        cos_z       = np.cos(zenith_norm * (np.pi / 2))
        airmass     = 1.0 / np.maximum(cos_z, 0.05)        # avoid division by zero near 90°
        extinction  = np.exp(-p["extinction_k"] * (airmass - 1.0)).astype(np.float32)

        # FIX #13 — twinkle sigma scales with altitude (more near horizon)
        twinkle_sigma = (0.05 + 0.25 * zenith_norm).astype(np.float32)
        twinkle = np.random.normal(1.0, twinkle_sigma).astype(np.float32)

        current_colors = self.stars_colors[valid_indices] * (extinction * twinkle)[:, np.newaxis]

        # FIX #7 — direct index instead of np.isin (O(n) vs O(n log n))
        rads = self.star_rads[valid_indices]
        is_small = rads == 1

        # Small (1-px) stars — simple scatter
        if np.any(is_small):
            sx = vx[is_small]
            sy = vy[is_small]
            self.frame_buffer[sy, sx] += current_colors[is_small]

        # Large stars — stamp-based (FIX #6, no per-star Python loop)
        for r_val in [2, 3]:
            mask_r = rads == r_val
            if not np.any(mask_r):
                continue
            stamp = self.star_stamps[r_val]           # pre-computed circular mask
            half  = r_val
            lx = vx[mask_r]
            ly = vy[mask_r]
            lc = current_colors[mask_r]

            for i in range(len(lx)):
                x0, y0 = lx[i] - half, ly[i] - half
                x1, y1 = x0 + stamp.shape[1], y0 + stamp.shape[0]

                # Clamp to frame bounds
                fx0, fy0 = max(x0, 0), max(y0, 0)
                fx1, fy1 = min(x1, w), min(y1, h)
                sx0, sy0 = fx0 - x0, fy0 - y0
                sx1, sy1 = sx0 + (fx1 - fx0), sy0 + (fy1 - fy0)

                if fx1 <= fx0 or fy1 <= fy0:
                    continue

                patch  = stamp[sy0:sy1, sx0:sx1, np.newaxis]
                colour = lc[i].reshape(1, 1, 3)
                self.frame_buffer[fy0:fy1, fx0:fx1] += patch * colour

    def _render_moon(self):
        """
        Draw moon disk + soft radial glow + earthshine.
        - Glow is a proper Gaussian radial falloff added onto the frame (not a
          solid filled circle which creates dark rings).
        - Moon disk has a minimum brightness so it is always visible even at
          new moon phase (earthshine keeps it dimly lit).
        - Arc sweeps left-to-right so the moon rises on the left, peaks at
          zenith, and sets on the right — matching real sky behaviour.
        """
        p     = self.sim_params
        illum, waxing = self.moon_phase_data
        h, w  = self.height, self.width
        r     = p["moon_radius_pixels"]

        # Arc parameter: 0.0 (left/rise) -> 1.0 (right/set)
        t = (self.frame_count % p["moon_arc_period"] / p["moon_arc_period"])

        # Horizontal sweep left to right with glow-radius margin
        margin = p["moon_glow_radius"] + r
        cx = int(margin + (w - 2 * margin) * t)

        # Vertical sine arc: derive pixel positions from frame height fractions
        peak_y    = p["moon_arc_peak_frac"]    * h
        horizon_y = p["moon_arc_horizon_frac"] * h
        arc_y     = horizon_y + (peak_y - horizon_y) * np.sin(t * np.pi)
        cy        = int(np.clip(arc_y, margin, h - margin))

        # --- Soft radial glow via Gaussian falloff added onto frame ---
        # Build a small glow patch and add it (no hard edges, no dark rings)
        gr    = p["moon_glow_radius"]
        gsize = gr * 2 + 1
        sigma = gr / 2.5

        # Gaussian kernel
        ax    = np.arange(gsize, dtype=np.float32) - gr
        gauss = np.exp(-(ax ** 2) / (2 * sigma ** 2))
        patch = np.outer(gauss, gauss)
        patch /= patch.max()

        # Scale by illumination — minimum glow even at new moon (earthshine)
        glow_illum = max(illum, 0.15)
        # BGR: slightly cool-white glow
        glow_bgr = np.stack([
            patch * 220 * p["moon_glow_strength"] * glow_illum,
            patch * 235 * p["moon_glow_strength"] * glow_illum,
            patch * 255 * p["moon_glow_strength"] * glow_illum,
        ], axis=-1).astype(np.float32)

        # Paste glow patch onto frame (additive blend)
        y0, x0 = cy - gr, cx - gr
        y1, x1 = cy + gr + 1, cx + gr + 1
        # Clip to frame bounds
        fy0, fx0 = max(y0, 0), max(x0, 0)
        fy1, fx1 = min(y1, h), min(x1, w)
        py0, px0 = fy0 - y0, fx0 - x0
        py1, px1 = py0 + (fy1 - fy0), px0 + (fx1 - fx0)
        self.frame_buffer[fy0:fy1, fx0:fx1] += glow_bgr[py0:py1, px0:px1]

        # --- Moon disk with proper phase terminator ---
        #
        # Clean algorithm — no flipping, no waxing/waning ambiguity:
        #
        #  phase 0.00 → new moon      : dark disk only
        #  phase 0.25 → first quarter : right half lit, terminator on left
        #  phase 0.50 → full moon     : entire disk lit
        #  phase 0.75 → last quarter  : left half lit, terminator on right
        #  phase 1.00 → new moon      : dark disk only
        #
        # Steps:
        #  1. Fill full disk with dark (earthshine) colour
        #  2. Compute terminator ellipse x-radius from phase
        #  3. phase < 0.5  → lit half is RIGHT  → draw right half-ellipse lit
        #                                        → draw right half of terminator dark
        #  4. phase >= 0.5 → lit half is LEFT   → draw left  half-ellipse lit
        #                                        → draw left  half of terminator dark
        #  5. Limb darkening ring

        phase = p["moon_phase_override"] if p["moon_phase_override"] >= 0 else illum

        earthshine = p["moon_earthshine_strength"] * (1.0 - illum)

        # Colours (BGR)
        dark_val = max(8, int(earthshine * 255))
        dark_col = (dark_val, dark_val, dark_val)

        lit_b = min(255, int(235))
        lit_g = min(255, int(245))
        lit_r = min(255, int(255))
        lit_col = (lit_b, lit_g, lit_r)

        # Patch canvas — work at pixel level, paste onto frame at the end
        pad   = r + 2
        psize = pad * 2 + 1
        patch = np.zeros((psize, psize, 3), dtype=np.float32)
        pc    = (pad, pad)

        # Step 1: full dark disk
        cv2.circle(patch, pc, r, dark_col, -1, cv2.LINE_AA)

        # --- Terminator logic ---
        # The lit area always grows from RIGHT to LEFT continuously:
        #
        #  phase 0.00 → new moon:      dark disk, no lit area
        #  phase 0.25 → crescent:      thin lit sliver on right
        #  phase 0.50 → first quarter: right half lit, terminator at centre
        #  phase 0.75 → gibbous:       most of disk lit, thin dark crescent left
        #  phase 1.00 → full moon:     entire disk lit
        #
        # The terminator is always a vertical ellipse sweeping left.
        # Its x-radius goes from r (phase=0) to 0 (phase=0.5) to -r (phase=1).
        # We map phase 0→1 to term_x ranging from +r to -r (linear).
        # Positive term_x → terminator bulges RIGHT (crescent phase)
        # Negative term_x → terminator bulges LEFT  (gibbous phase)
        #
        # Implementation:
        #   Always draw RIGHT half-disk as lit.
        #   Then draw terminator ellipse on the RIGHT side with dark colour
        #   (phase < 0.5) or lit colour on the LEFT side (phase > 0.5).

        # Always light the right half first
        cv2.ellipse(patch, pc, (r, r), 0, -90, 90, lit_col, -1, cv2.LINE_AA)

        if phase < 0.5:
            # Crescent: terminator cuts into the right lit half
            # term_rx shrinks from r→0 as phase goes 0→0.5
            term_rx = int(r * (1.0 - phase * 2))
            if term_rx > 0:
                cv2.ellipse(patch, pc, (term_rx, r), 0, -90, 90, dark_col, -1, cv2.LINE_AA)

        elif phase > 0.5:
            # Gibbous: extend lit area into the left dark half
            # term_rx grows from 0→r as phase goes 0.5→1.0
            term_rx = int(r * ((phase - 0.5) * 2))
            if term_rx > 0:
                cv2.ellipse(patch, pc, (term_rx, r), 0, 90, 270, lit_col, -1, cv2.LINE_AA)

        # phase == 0.5 → exactly right half lit, no terminator needed

        # Subtle limb darkening ring
        limb_col = (int(lit_b * 0.55), int(lit_g * 0.55), int(lit_r * 0.55))
        cv2.circle(patch, pc, r, limb_col, 1, cv2.LINE_AA)

        # Paste patch onto frame buffer.
        # Use a disk mask so the moon OVERWRITES whatever is beneath it
        # (stars, noise) — the moon is an opaque body that occludes everything.
        py0, px0 = cy - pad, cx - pad
        py1, px1 = py0 + psize, px0 + psize
        fy0, fx0 = max(py0, 0), max(px0, 0)
        fy1, fx1 = min(py1, h), min(px1, w)
        sy0, sx0 = fy0 - py0, fx0 - px0
        sy1, sx1 = sy0 + (fy1 - fy0), sx0 + (fx1 - fx0)
        if fy1 > fy0 and fx1 > fx0:
            # Build a binary disk mask for the moon circle
            disk_mask = np.zeros((psize, psize), dtype=np.float32)
            cv2.circle(disk_mask, pc, r, 1.0, -1, cv2.LINE_AA)
            m = disk_mask[sy0:sy1, sx0:sx1, np.newaxis]   # (h, w, 1) in [0,1]

            # Inside disk: overwrite frame with moon patch (occludes stars)
            # Outside disk: keep frame as-is, blend in any glow (already added above)
            fb = self.frame_buffer[fy0:fy1, fx0:fx1]
            self.frame_buffer[fy0:fy1, fx0:fx1] = fb * (1.0 - m) + patch[sy0:sy1, sx0:sx1] * m

    def _render_clouds(self):
        """Drift and blend all cloud layers."""
        for layer_info in self.cloud_layers:
            self._process_cloud_layer(layer_info)

    def _render_streaks(self):
        """Trigger and advance satellite / airplane streaks."""
        self._process_streaks()

    def _render_meteors(self):
        """Trigger and advance meteor events."""
        p = self.sim_params
        if not self.meteor_active and random.random() < 1.0 / p["meteor_interval"]:
            self._start_meteor()
        if self.meteor_active:
            self._draw_meteor()

    def _render_vignette(self):
        """Apply lens vignette darkening at frame edges."""
        self.frame_buffer *= self.vignette_map

    def _process_cloud_layer(self, layer_info: dict):
        params = layer_info["params"]
        layer_info["offset_x"] += params["speed"][0]
        layer_info["offset_y"] += params["speed"][1]

        if self.frame_count - layer_info["last_reseed"] > params["reseed"]:
            self._generate_cloud_layer(layer_info["data"], params)
            layer_info["last_reseed"] = self.frame_count
            logging.debug("SIM: Reseeding cloud layer.")

        # FIX #18 — np.roll wraps the tileable canvas: whatever drifts off the
        # right edge reappears on the left (and same vertically). No seams because
        # the canvas was generated with tiling neighbours in _generate_cloud_layer.
        ox = int(layer_info["offset_x"]) % self.width
        oy = int(layer_info["offset_y"]) % self.height
        drifted = np.roll(layer_info["data"], shift=(oy, ox), axis=(0, 1))

        # FIX #4 — Blend toward grey cloud colour instead of multiplying to black
        cloud_colour = np.array(self.sim_params["cloud_grey"], dtype=np.float32)
        alpha        = drifted[..., np.newaxis]
        self.frame_buffer = self.frame_buffer * (1.0 - alpha) + cloud_colour * alpha

    def _process_streaks(self):
        p = self.sim_params
        if not self.streak_active:
            if random.random() < 1.0 / p["satellite_interval"]:
                self._start_streak("satellite")
            elif random.random() < 1.0 / p["airplane_interval"]:
                self._start_streak("airplane")
            return

        # FIX #12 — Airplane strobes cycle red → green → white (realistic nav lights)
        if self.streak_type == "airplane":
            strobe_colours = [          # BGR
                ( 50,  50, 255),   # red
                ( 50, 255,  50),   # green
                (255, 255, 255),   # white
            ]
            self.streak_color = strobe_colours[(self.streak_frame // 3) % 3]

        if not (self.streak_dash and (self.streak_frame % 6 < 3)):
            p1 = tuple(self.streak_pos.astype(int))
            p2 = tuple((self.streak_pos + self.streak_vec * 2).astype(int))
            cv2.line(self.frame_buffer, p1, p2, self.streak_color, self.streak_thickness)

        self.streak_pos   += self.streak_vec
        self.streak_frame += 1

        if self.streak_frame > p["streak_duration"]:
            self.streak_active = False
            logging.info(f"SIM: {self.streak_type.capitalize()} object exited view.")

    def _start_streak(self, s_type: str):
        logging.info(f"SIM: >>> GENERATING {s_type.upper()} STREAK <<<")
        self.streak_active    = True
        self.streak_type      = s_type
        self.streak_frame     = 0
        self.streak_pos[:]    = [
            np.random.uniform(0, self.width),
            np.random.uniform(0, self.height),
        ]
        angle              = np.random.uniform(0, 2 * np.pi)
        speed              = np.random.uniform(8, 20)
        self.streak_vec[:] = [np.cos(angle) * speed, np.sin(angle) * speed]

        if s_type == "satellite":
            self.streak_color     = (255, 255, 255)  # white BGR
            self.streak_thickness = 1
            self.streak_dash      = False
        else:   # airplane
            # Initial colour; will cycle each frame via strobe logic
            self.streak_color     = (50, 50, 255)  # red BGR
            self.streak_thickness = 3
            self.streak_dash      = True

    def _start_meteor(self):
        logging.info("SIM: ☄️  >>> TRIGGERING METEOR EVENT <<<")
        self.meteor_active = True
        self.meteor_frame  = 0
        self.meteor_trail  = []
        p = self.sim_params

        edge = random.choice(["top", "left", "right"])
        if edge == "top":
            x, y = np.random.uniform(0, self.width), 0.0
            vx   = np.random.uniform(-8, 8)
            vy   = np.random.uniform(p["meteor_speed_min"], p["meteor_speed_max"])
        elif edge == "left":
            x, y = 0.0, np.random.uniform(0, self.height * 0.6)
            vx   = np.random.uniform(10, p["meteor_speed_max"])
            vy   = np.random.uniform(-10, 10)
        else:
            x, y = float(self.width), np.random.uniform(0, self.height * 0.6)
            vx   = np.random.uniform(-p["meteor_speed_max"], -10)
            vy   = np.random.uniform(-10, 10)

        self.meteor_pos[:] = [x, y]
        self.meteor_vec    = np.array([vx, vy], dtype=np.float32)
        self.meteor_peak   = np.random.uniform(200, 255)
        self.meteor_thick  = np.random.randint(p["meteor_thickness_min"], p["meteor_thickness_max"] + 1)

        c = random.random()
        # BGR order
        if c < 0.70:   self.meteor_col = (255, 255, 255)  # white
        elif c < 0.90: self.meteor_col = (200, 255, 200)  # greenish-white
        else:          self.meteor_col = (200, 220, 255)  # warm white

    def _draw_meteor(self):
        p          = self.sim_params
        self.meteor_frame += 1
        prev_pos   = self.meteor_pos.copy()
        self.meteor_pos += self.meteor_vec

        progress    = self.meteor_frame / p["meteor_duration"]
        head_bright = self.meteor_peak * (1 - progress)

        if head_bright > 0:
            cv2.line(
                self.frame_buffer,
                tuple(prev_pos.astype(int)),
                tuple(self.meteor_pos.astype(int)),
                self.meteor_col,
                self.meteor_thick + 1,
            )

        self.meteor_trail.append({
            "p1":  prev_pos,
            "p2":  self.meteor_pos.copy(),
            "b":   head_bright * 1.2,
            "age": 0,
        })
        # Age-out stale trail segments
        self.meteor_trail = [t for t in self.meteor_trail if t["age"] < p["meteor_trail_length"]]

        for t in self.meteor_trail:
            t["age"] += 1
            fade = 1.0 - (t["age"] / p["meteor_trail_length"])
            if fade > 0 and t["b"] > 0:
                col = tuple(ch * fade * t["b"] / 255 for ch in self.meteor_col)
                cv2.line(
                    self.frame_buffer,
                    tuple(t["p1"].astype(int)),
                    tuple(t["p2"].astype(int)),
                    col,
                    self.meteor_thick,
                )

        if self.meteor_frame > p["meteor_duration"] and not self.meteor_trail:
            self.meteor_active = False
            logging.info("SIM: Meteor event ended.")

    def _get_moon_phase_fraction(self) -> Tuple[float, bool]:
        # Use override if set; otherwise derive from real date
        p = self.sim_params
        if p["moon_phase_override"] >= 0:
            frac  = p["moon_phase_override"]
        else:
            now   = datetime.now()
            ref   = datetime(2000, 1, 6, 18, 14, 0)
            days  = (now - ref).total_seconds() / 86400.0
            cycle = 29.530_588_853
            frac  = (days % cycle) / cycle
        illum = (1 - np.cos(frac * 2 * np.pi)) / 2
        return float(illum), frac < 0.5

    def get_metadata(self) -> dict:
        return {"Lux": 500.0 if self.is_day_time else 0.5}

    def release(self):
        pass

    def flush(self, count: int = 4):
        # FIX #17 — respect actual exposure_us, not a magic 0.05 constant
        time.sleep((self.exposure_us / 1e6) * count)
    # --------------------------- SFTP connect and upload ---------------------------
class SFTPUploader:
    def __init__(self, cfg, pipeline_instance):
        self.cfg = cfg
        self.pipeline_instance = pipeline_instance
        # self.upload_q = queue.Queue()
        self.stop_event = threading.Event()
        self.sftp_password = os.environ.get("SFTP_PASS")
        self.upload_q = queue.Queue(maxsize=cfg.get("max_queue_size", 500)) 
        
        if self.sftp_password:
            logging.info("SFTP password loaded from SFTP_PASS environment variable.")
        else:
            # If the password environment variable is missing, log an error.
            logging.error("SFTP_PASS environment variable is not set. SFTP uploads will fail.")

    def _ensure_remote_dir(self, sftp_client):
        remote_dir = self.cfg['remote_dir']
        try:
            # Check if the directory already exists
            sftp_client.stat(remote_dir)
            logging.info(f"Remote directory '{remote_dir}' already exists.")
        except FileNotFoundError:
            # If it doesn't exist, create it.
            logging.warning(f"Remote directory '{remote_dir}' not found. Attempting to create it.")
            try:
                # Create directories recursively
                dirs = remote_dir.strip('/').split('/')
                current_dir = ''
                for d in dirs:
                    current_dir = os.path.join(current_dir, d)
                    # For SFTP, paths should always use forward slashes
                    current_dir_posix = current_dir.replace(os.path.sep, '/') 
                    if not current_dir_posix.startswith('/'):
                        current_dir_posix = '/' + current_dir_posix
                    try:
                        sftp_client.stat(current_dir_posix)
                    except FileNotFoundError:
                        logging.info(f"Creating remote directory: {current_dir_posix}")
                        sftp_client.mkdir(current_dir_posix)
            except Exception as e:
                logging.error(f"Failed to create remote directory '{remote_dir}': {e}")
                # Re-raise the exception to cause the connection to fail.
                raise

    def connect(self):
        try:
            # Use the higher-level SSHClient which simplifies connection and has a timeout
            client = paramiko.SSHClient()
            # Automatically add the server's host key. For high-security environments,
            # you might want to load known host keys instead.
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())            
            
            client.connect(
                hostname=self.cfg['host'],
                port=self.cfg['port'],
                username=self.cfg['user'],
                password=self.sftp_password,
                timeout=30
            )

            sftp = client.open_sftp()
            logging.info("SFTP connection established.")
            
            self._ensure_remote_dir(sftp)
            
            # Return both the client and sftp object for proper cleanup later
            return client, sftp
        
        except Exception as e:
            # This will now catch timeout errors, authentication errors, and name resolution errors.
            msg = f"SFTP connection failed: {e}"
            logging.error(msg)
            self.pipeline_instance.log_health_event("WARNING", "SFTP_connection_failed", msg)
            return None, None

    def get_status(self):
        """Returns a tuple of (status_class, status_message) for the dashboard."""
        q_size = self.upload_q.qsize()
        
        # Check for power loss first, as it's the highest priority status
        with self.pipeline_instance.status_lock:
            if self.pipeline_instance.power_status != "OK":
                return ("err", f"Paused (Power Loss) - Queue: {q_size}")

        q_max = self.upload_q.maxsize
        if q_max == 0: return ("ok", f"Queue: {q_size}") # Should not happen, but safe

        q_percent = (q_size / q_max) * 100
        
        if q_percent >= 99:
            return ("err", f"Backlogged - Queue: {q_size}/{q_max} ({q_percent:.0f}%)")
        elif q_percent > 75:
            return ("warn", f"Filling - Queue: {q_size}/{q_max} ({q_percent:.0f}%)")
        else:
            return ("ok", f"Idle/Uploading - Queue: {q_size}")

    def worker_loop(self):
        ssh_client = None
        sftp = None
        was_paused_by_power_loss = False
        
        INITIAL_RECONNECT_DELAY_SEC = 30  # Start with a 30-second wait
        MAX_RECONNECT_DELAY_SEC = 1800    # Cap at 30 minutes (30 * 60)
        current_reconnect_delay = INITIAL_RECONNECT_DELAY_SEC
        
        while not self.stop_event.is_set():

            # --- PRIORITY 1: CHECK FOR POWER LOSS ---
            if self.pipeline_instance.power_status != "OK":
                if not was_paused_by_power_loss:
                    logging.warning("SFTP Uploader: Power loss detected. Pausing all upload activity.")
                    was_paused_by_power_loss = True
                
                # If a connection exists, close it to be safe during a power outage.
                if sftp:
                    sftp.close()
                    sftp = None
                    
                self.stop_event.wait(timeout=10) 
                continue # Restart the loop to re-check power status.
            
            # If we are here, power is OK. Check if it was just restored.
            if was_paused_by_power_loss:
                logging.info("SFTP Uploader: Power has been restored. Resuming uploads.")
                was_paused_by_power_loss = False
                # sftp is already None, so the next block will force a reconnect.
                current_reconnect_delay = INITIAL_RECONNECT_DELAY_SEC

            # --- PRIORITY 2: GET A CONNECTION (STATE 1) ---
            if sftp is None:
                logging.info("SFTP: No active connection. Attempting to connect...")
                ssh_client, sftp = self.connect()
                
                if sftp is None:
                    # Connection failed. Wait without touching the queue.
                    logging.warning(f"SFTP: Connection failed. Will retry in {current_reconnect_delay} seconds.")
                    self.stop_event.wait(timeout=current_reconnect_delay)
                    
                    # Increase the delay for the next attempt, up to the maximum
                    current_reconnect_delay = min(current_reconnect_delay * 2, MAX_RECONNECT_DELAY_SEC)
                    continue
                else:
                    # --- NEW: Reset Backoff Logic on Success ---
                    logging.info("SFTP: Connection established successfully. Resetting reconnect delay.")
                    current_reconnect_delay = INITIAL_RECONNECT_DELAY_SEC
                    logging.info("SFTP: Starting to process upload queue.")

            # --- PRIORITY 3: PROCESS THE QUEUE (STATE 2) ---
            # Now that we know power is OK and we have a connection, we can get a file.
            try:
                local_path = self.upload_q.get(timeout=5)
                if local_path is None: continue

            except queue.Empty:
                # No files to upload.
                continue

            # Attempt the upload.
            try:
                remote_path = os.path.join(self.cfg['remote_dir'], os.path.basename(local_path))
                sftp.put(local_path, remote_path)
                logging.info(f"Uploaded: {os.path.basename(local_path)}")
                self.mark_as_uploaded(local_path)

            except Exception as e:
                # An error occurred during the upload. The connection is likely now broken.

                msg = f"Upload failed for {os.path.basename(local_path)}: {e}"
                logging.error(msg)
                
                self.pipeline_instance.log_health_event("ERROR", "SFTP_UPLOAD_FAIL", msg)
                
                logging.warning("SFTP connection may have been lost. Re-queueing file and forcing reconnect.")
                
                # Re-queue the file so it's not lost.
                self.upload_q.put(local_path) 
                
                # Close the broken connection and set sftp to None.
                # This will force to go back to STATE 1 on the next iteration.
                if sftp: sftp.close()
                if ssh_client: ssh_client.close()
                sftp, ssh_client = None, None
        
        # Final cleanup on shutdown.
        if sftp: 
            sftp.close()
            logging.info("SFTP connection closed.")
            
        if ssh_client: ssh_client.close()

    def mark_as_uploaded(self, file_path):
        # Marks a file's metadata as uploaded.
        json_path = os.path.splitext(file_path)[0] + ".json"
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r+') as f:
                    meta = json.load(f)
                    meta["uploaded_at"] = datetime.now(timezone.utc).isoformat()
                    self.pipeline_instance.save_json_atomic(json_path, meta)
            except Exception as e:
                logging.error(f"Failed to mark {os.path.basename(json_path)} as uploaded: {e}")

    def stop(self):
            """Signals the SFTP worker to stop."""
            self.stop_event.set()
            # Post a sentinel to unblock the worker_loop if it's waiting on the queue
            try: self.upload_q.put_nowait(None)
            except queue.Full: pass
    # -----------------
class SkyConditionStatus(Enum):
    # Represents the possible sky condition check result.
    CLEAR = auto()
    CLOUDY = auto()
    ERROR = auto()
    # --------------------------- Pipeline class and workers ---------------------------
class Pipeline:
    # -----------------
    # 1. INITIALIZATION & CONFIGURATION
    # -----------------
    def __init__(self, cfg, config_path=None, simulate=False):

        self.cfg = cfg
        
        self.config_path = config_path or "config.json"  # config path
        self._apply_resolution_aware_tuning()
        
        # Get the base path from the janitor config. Default to the script's directory.
        base_path = self.cfg["janitor"].get("monitor_path", ".")    
        
        # Set the paths and files.
        self.events_out_dir = os.path.join(base_path, "output/events")
        self.timelapse_out_dir = os.path.join(base_path, "output/timelapse")
        self.daylight_out_dir = os.path.join(base_path, "output/daylight")                                                                                           
        self.calibration_out_dir = os.path.join(base_path, "output/calibration")
        self.general_log_dir = os.path.join(base_path, self.cfg["general"]["log_dir"])
        self.monitor_out_file = os.path.join(self.general_log_dir, "system_monitor.csv")
        self.event_log_out_file = os.path.join(self.general_log_dir, "events.csv")
        self.health_log_out_file = os.path.join(self.general_log_dir, "health_stats.csv")
        self.backlog_file_path = os.path.join(self.general_log_dir, "sftp_backlog.txt")
        self.camera_k_file = os.path.join(self.general_log_dir, "camera_k.csv")
        self.masterpiece_archive_dir = os.path.join(self.daylight_out_dir, "archive")
 
        os.makedirs(self.events_out_dir, exist_ok=True)
        os.makedirs(self.timelapse_out_dir, exist_ok=True)
        os.makedirs(self.general_log_dir, exist_ok=True)
        os.makedirs(self.calibration_out_dir, exist_ok=True)
        os.makedirs(self.daylight_out_dir, exist_ok=True)
        os.makedirs(self.masterpiece_archive_dir, exist_ok=True)
        
        # dummy = np.zeros((self.cfg["capture"]["height"], self.cfg["capture"]["width"], 3), dtype=np.uint8)
        # cv2.imwrite(os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.jpg"), dummy)
 
        # Global stop signal for the entire application
        self.running = threading.Event()
        self.running.set()
        
        # Separate stop signal for the capture workers
        self.capture_running = threading.Event()
        self.capture_running.set()

        # Lists to manage different types of threads
        self.worker_threads = []
        self.control_threads = []

        # maxq = cfg["general"].get("max_queue_size", 1000)
        self.acq_q = queue.Queue(maxsize=self.effective_max_q)          # Queue for the acquisition frame
        self.event_q = queue.Queue()                    # Queue for the event frame
        self.timelapse_q = queue.Queue(maxsize=self.effective_max_q)    # Queue for timelapse frame
        self.event_stack_q = queue.Queue()              # Queue for event stacks
        self.event_log_q = queue.Queue()                # Queue for the event logger
        self.health_q = queue.Queue()                   # Queue for health statistics
        self.calibration_q = queue.Queue(maxsize=100)   # Queue for the calibration image
        self.sftp_dispatch_q = queue.Queue()            # Queue for SFTP dispatcher
        
        self.last_calibration_image_path = None
        self.is_calibrating = threading.Event()
        self.is_calibrating.clear()
        self.event_counter = 0

        # self.debug_stack_counter = 0 
        # self.debug_stack_limit = 5

        if args.simulate:
            # Use the fake camera
            self.camera = FrameSimulator(cfg["capture"])
        else:
            # Use the real camera
            self.camera = CameraCapture(cfg["capture"])

        self.background = None
        self.camera_fatal_error = threading.Event()     # Set by capture_loop if fatal
        self.consecutive_camera_failures = 0
        self.consecutive_restart_failures = 0
        self.config_reloaded_ack = threading.Event()    # Triggers for config reload

        # --- Initialize the SFTP ---
        self.sftp_uploader = None
        if self.cfg["sftp"].get("enabled", False):
            try:
                # Dependencies
                # import paramiko 
                self.sftp_uploader = SFTPUploader(self.cfg["sftp"], self)
            except ImportError:
                logging.error("SFTP is enabled, but 'paramiko' library is not installed. Please run 'pip3 install paramiko'.")
            except Exception as e:
                logging.error(f"Failed to initialize SFTP uploader: {e}")
        
        # --- Initialize status LED ---
        self.led_line = None
        if IS_GPIO_AVAILABLE and self.cfg["led_status"].get("enabled", False):
            try:
                self.led_pin = self.cfg["led_status"]["pin"]
                
                # 1. Create a LineSettings object to define the line's configuration.
                settings = gpiod.LineSettings(
                    direction=gpiod.line.Direction.OUTPUT,
                    output_value=gpiod.line.Value.ACTIVE  # Set initial value to ON (booting)
                )
                
                # 2. Request the line with the specified settings.
                self.led_line = GPIO_CHIP.request_lines(
                    consumer="meteora-pipeline-led",
                    config={self.led_pin: settings}
                )
                logging.info(f"Status LED enabled on GPIO pin {self.led_pin}. LED is ON for startup sequence.")

            except Exception as e:
                logging.error(f"Failed to initialize status LED on pin {self.led_pin}: {e}", exc_info=True)
                if self.led_line: self.led_line.release() # Ensure release on failure
                self.led_line = None
        else:
            logging.info("Status LED is disabled.")

        self._load_calibration_frames()                      
        
        ctx = zmq.Context()
        self.debug_pub = ctx.socket(zmq.PUB)
        try: 
            self.debug_pub.bind("tcp://*:5555")
        except zmq.ZMQError:
            logging.info("ZMQ port 5555 already in use. Debug publisher disabled.")

        self.last_night_journal = "Initial sequence started. The first chronicle will be written at sunrise."
        self.session_stats = self._reset_session_stats()            
        
        # The Master Canvas: Persistent Maximum Intensity Projection (MIP).
        mip_path = os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.png")
        if os.path.exists(mip_path):
            # Resume drawing on the existing image!
            self.nightly_masterpiece = cv2.imread(mip_path, cv2.IMREAD_UNCHANGED)
            logging.info("Restored existing Nightly Masterpiece from disk.")
        else:
            # Only create a blank black canvas if no file exists
            self.nightly_masterpiece = None
            dummy = np.zeros((self.cfg["capture"]["height"], self.cfg["capture"]["width"], 3), dtype=np.uint16)
            cv2.imwrite(mip_path, dummy)
        
        self.sky_score = 0.0
        self.daylight_mode_active = threading.Event()
        self.daylight_mode_active.clear()               # Default to night mode (detection on)
        # self.mode_check_lock = threading.Lock()         # Lock to prevent race conditions during mode checks
        self.timelapse_interval_sec = self.cfg["timelapse"].get("daymode_interval_sec", 0)
        self.base_timelapse_interval = self.timelapse_interval_sec
        self.timelapse_frames_queued = 0    
        self.emergency_frame_throttle = 0.0
        self.weather_hold_active = threading.Event()
        self.weather_hold_active.clear()                # Start in good weather status
        self.event_in_progress = threading.Event()
        self.event_in_progress.clear()                  # Start in the "no event" state
        self.reload_config_signal = threading.Event()
        self.request_sky_analysis_trigger = threading.Event()
        self.status_lock = threading.Lock()             # A dedicated lock for status variables
        self.last_illuminance = "N/A"
        self.last_sky_conditions = {"stddev": "N/A", "stars": "N/A"}
        self.last_moon_status = {"impact": 0.0, "in_fov": False, "alt": 0.0, "dist": 180.0}
        self.power_status = "DISABLED"                        # Can be OK, LOST, SHUTTING_DOWN, or DISABLED
        self.last_heartbeat_status = "N/A"  
        self.noise_baseline_stddev = None
        self.last_effective_threshold = "N/A"
        self.effective_threshold = None
#        self.noise_alpha = self.cfg["detection"].get("noise_smoothing_alpha", 0.15)
        self.threshold_min = self.cfg["detection"].get("min_threshold", 10)
        self.threshold_max = self.cfg["detection"].get("max_threshold", 255)
        self.last_moon_info = None
        self.last_calibration_error = None
        self.last_event_files = {"image": None, "video": None}
        self.last_queue_status = ("ok", "Starting...")  # Holds (css_class, message)
        self.timelapse_next_capture_time = 0.0          # Control variable for timelapse timing
        self.capture_stable = threading.Event()
        self.session_start_time = None
        self.camera_lock = threading.Lock()
        self.current_exposure_us = self.cfg["capture"].get("exposure_us")
        self.current_gain = self.cfg["capture"].get("gain")                                            
        self.in_maintenance_mode = threading.Event()
        self.in_maintenance_mode.clear()                # Start in normal operating mode
        self.watchdog_pause = threading.Event()
        self.emergency_stop_active = threading.Event()
        self.emergency_stop_active.clear()              # Set in the "not stopped" state
        self.initial_sftp_sweep_complete = threading.Event()
        self.initial_sftp_sweep_complete.clear()        # Start in the "not complete" state
        self.initial_janitor_check_complete = threading.Event()
        self.timelapse_complete_event = threading.Event()
        self.timelapse_complete_event.set()         # Set initially to allow first run
        self.initial_janitor_check_complete.clear()     # Start in the "not complete" state
        self.system_ready = threading.Event() # Master "All Systems Go" signal
        self.maintenance_timeout_lock = threading.Lock()
        self.maintenance_timeout_until = 0              # A timestamp indicating when the timeout expires
        self.maintenance_timeout_duration_sec = cfg["general"].get("maintenance_timeout", 300)
        self.last_sky_status = None
        self.last_daylight_mode = None
        self.background_reset_time = 0
        self.calibration_k = 0.0

        # A list of editable checkbox in the dashboard - enable / disable the relative value in the config file
        self.editable_booleans = ["auto_exposure_tuning", "auto_sensitivity_tuning", "stack_align", "astro_stretch"]

        self._initial_cleanup()
    # -----------------
    def _setup_logging(self):
        """Configures the root logger, ensuring all timestamps are in UTC."""
        log_dir = self.general_log_dir
        max_mb = self.cfg.get("janitor", {}).get("log_rotation_mb", 10)
        backup_count = self.cfg.get("janitor", {}).get("log_backup_count", 5)

        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "pipeline.log")

        utc_formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")
        utc_formatter.converter = time.gmtime

        color_utc_formatter = ThreadColorLogFormatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")
        color_utc_formatter.converter = time.gmtime

        file_handler = RotatingFileHandler(log_path, maxBytes=max_mb * 1024 * 1024, backupCount=backup_count)
        file_handler.setFormatter(utc_formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_utc_formatter)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if logger.hasHandlers():
            logger.handlers.clear()
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logging.info("Logging started. All timestamps will be in UTC.")
    # -----------------
    def _initial_cleanup(self):
        # Scans and removes orphaned temporary directories.
        logging.info("Performing initial cleanup check...")
        event_dir = self.events_out_dir
        cleanup_count = 0
        if os.path.isdir(event_dir):
            try:
                with os.scandir(event_dir) as it:
                    for entry in it:
                        # Temporary directories are our specific target
                        if entry.is_dir() and entry.name.endswith('_frames'):
                            logging.warning("Found orphaned temp directory from previous run: %s. Deleting.", entry.name)
                            try:
                                shutil.rmtree(entry.path)
                                cleanup_count += 1
                            except Exception:
                                logging.exception("Error deleting orphaned directory %s", entry.path)
            except Exception:
                logging.exception("Error during initial cleanup scan.")
        if cleanup_count > 0:
            logging.info("Initial cleanup removed %d orphaned directories.", cleanup_count)
    # -----------------
    def _reset_session_stats(self):
        return {
            "start_time": datetime.now(timezone.utc),
            "last_check_time": time.time(),
            "total_events": 0,
            "rejected_contours": 0,
            "accepted_events": 0,
            "clear_sky_minutes": 0,
            "cloudy_sky_minutes": 0,
            "error_mins": 0,
            "avg_sky_score": [],
            "max_moon_impact": 0.0,
            "moon_visible_minutes": 0,
            "data_written_mb": 0.0,
            "peak_cpu_temp": 0.0,
            "avg_lux": []
        }
    # -----------------
    def _apply_resolution_aware_tuning(self):
        """Automatically optimizes logic variables based on the total pixel count."""
        w = self.cfg["capture"]["width"]
        h = self.cfg["capture"]["height"]
        mp = (w * h) / 1_000_000.0  # Megapixels

        # Find the profile that matches the resolution closest
        available_profiles = sorted(RESOLUTION_TUNING_PROFILES.keys())
        # Default to the lowest profile
        best_match_mp = available_profiles[0]
        for p_mp in available_profiles:
            if mp >= p_mp:
                best_match_mp = p_mp
        
        profile = RESOLUTION_TUNING_PROFILES[best_match_mp]
        
        # logging.warning(f"--- AUTO-TUNING ACTIVATED ({w}x{h} = {mp:.1f}MP) ---")
        logging.warning(f"Applying '{best_match_mp}MP' Tuning Profile.")
        # logging.warning(f"--- RESOLUTION AWARE ENGINE: {mp:.1f}MP detected ---")

        self.min_area = profile["min_area"]
        self.min_changes_required = profile["min_changes"]
        self.star_mask_radius = profile["radius"]
        self.alignment_contrast_floor = profile["std_floor"]
        user_max_q = self.cfg["general"].get("max_queue_size", 1000)
        self.effective_max_q = min(user_max_q, profile["max_q"])
        
        logging.info(f"Parameters set: Area>{self.min_area}, Motion>{self.min_changes_required}, MaskRadius={self.star_mask_radius}")
    # -----------------
    # 2. LIFECYCLE & MASTER SCHEDULER
    # -----------------
    def run(self):
        """
        The main entry point to start and run the entire pipeline application.
        """
        self._setup_logging()
        
        shutdown_event = threading.Event()
        def handle_signal(sig, frame):
            if not shutdown_event.is_set():
                logging.warning("Shutdown signal received! Initiating graceful shutdown...")
                self.stop()
                shutdown_event.set()
        
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        
        logging.info("="*60)
        logging.info(f"--- Meteora Pipeline v{self.cfg['general']['version']} Starting ---")
        logging.info("="*60)
        self.log_health_event("WARNING", "START", "The system is starting.")
        
        self.start()
        time.sleep(0.5)
        
        logging.info("Verifying thread health before engaging system...")
        all_threads = self.worker_threads + self.control_threads
        startup_failed = False
        
        for t in all_threads:
            if not t.is_alive():
                logging.critical(f"STARTUP ERROR: Thread '{t.name}' failed to start or died immediately.")
                startup_failed = True
        
        if startup_failed:
            logging.critical("Startup Aborted: One or more critical threads failed to start. Exiting.")
            self.stop()
            sys.exit(1)

        logging.info(f"Health Check Passed: All {len(all_threads)} threads are running.")
        
        # self._led_startup_signal()
        
        logging.info("All threads launched. Setting 'System Ready' signal.")
        self.system_ready.set()

        try:
            logging.info("Running pipeline indefinitely (Ctrl+C to stop).")
            shutdown_event.wait()
        finally:
            if not shutdown_event.is_set():
                logging.info("Main loop finished, initiating shutdown...")
                self.stop()

        logging.info("Pipeline has been stopped. Main thread exiting.")
        time.sleep(2)        
    # -----------------
    def start(self):
        logging.info("Starting all pipeline services...")
        power_pin = self.cfg["general"].get("power_monitor_pin", 17)

        self.worker_threads = [
            threading.Thread(target=self.event_writer_loop, name="EventWriterThread", daemon=True),
            threading.Thread(target=self.timelapse_loop, name="TimelapseThread", daemon=True),
            threading.Thread(target=self.event_stacker_loop, name="EventStackerThread", daemon=True),
            threading.Thread(target=self.calibration_worker_loop, name="CalibrationThread", daemon=True)
        ]

        if self.cfg["event_log"].get("enabled", False):
            self.worker_threads.append(threading.Thread(target=self.event_logger_loop, name="EventLoggerThread", daemon=True))
        else:
            logging.info("Event log is disabled in configuration.")        
        
        for t in self.worker_threads:
            t.start()
        logging.info("All worker threads started.")

        # 1. Add mandatory control threads
        self.control_threads = [
            threading.Thread(target=self.watchdog_loop, name="WatchdogThread", daemon=True),
            threading.Thread(target=self.system_monitor_loop, name="MonitorThread", daemon=True),
            threading.Thread(target=self.janitor_loop, name="JanitorThread", daemon=True),
            threading.Thread(target=self.scheduler_loop, name="SchedulerThread", daemon=True),
            threading.Thread(target=self.health_monitor_loop, name="HealthMonitorThread", daemon=True),
            threading.Thread(target=self.config_reloader_loop, name="ConfigReloaderThread", daemon=True)
        ]

        # 2. Add optional control threads based on configuration
              
        if self.cfg["power_monitor"].get("enabled", False):
            self.control_threads.append(threading.Thread(target=self.power_monitor_loop, name="PowerMonitorThread", daemon=True))
        else:
            logging.info("Power monitor is disabled in configuration.")
            self.power_status = "DISABLED"

        if self.cfg["heartbeat"].get("enabled", False):
            self.control_threads.append(threading.Thread(target=self.heartbeat_loop, name="HeartbeatThread", daemon=True))
        else:
            logging.info("Heartbeat is disabled in configuration.")

        if self.cfg["ntp"].get("enabled", False):
            self.control_threads.append(threading.Thread(target=self.ntp_sync_loop, name="NTPThread", daemon=True))
        else:
            logging.info("NTP is disabled in configuration.")

        if self.cfg["daylight"].get("enabled", False):
            self.control_threads.append(threading.Thread(target=self.sky_monitor_loop, name="SkyMonitorThread", daemon=True))
        else:
            logging.info("Sky monitor is disabled in configuration.")
            
        if self.cfg["dashboard"].get("enabled", False):
            self.control_threads.append(threading.Thread(target=self.dashboard_loop, name="DashboardThread", daemon=True))
            # The maintenance watchdog is tied to the dashboard's pause feature
            self.control_threads.append(threading.Thread(target=self.maintenance_watchdog_loop, name="MaintWatchdogThread", daemon=True))
        else:
            logging.info("Dashboard (and Maintenance Watchdog) is disabled in configuration.")
            
        if self.led_line:
            self.control_threads.append(threading.Thread(target=self.led_status_loop, name="LedStatusThread", daemon=True))
        else: 
            logging.info("Status LED disabled in configuration.")
                        
        for t in self.control_threads:
            t.start()
        logging.info("All control threads started.")       

        # 3. Add SFTP threads if enabled
        if self.sftp_uploader:
            sftp_thread = threading.Thread(target=self.sftp_uploader.worker_loop, name="SftpThread", daemon=True)
            sftp_thread.start()
            self.control_threads.append(sftp_thread) # Add to control threads for clean shutdown
            sftp_dispatcher_thread = threading.Thread(target=self.sftp_dispatcher_loop, name="SFTPDispatchThread", daemon=True)
            sftp_dispatcher_thread.start()
            self.control_threads.append(sftp_dispatcher_thread)
            logging.info("SFTP Uploader thread started.")

            # --- Perform the initial sweep AFTER threads are running but BEFORE capture starts ---
            try:
                logging.info("Performing mandatory startup sweep for un-uploaded files...")
                self.sweep_and_enqueue_unuploaded()
                logging.info("Startup SFTP sweep complete.")
            except Exception as e:
                logging.exception(f"An error occurred during startup SFTP sweep: {e}")
            finally:
                # CRITICAL: Always set the event, even if the sweep fails, to prevent a deadlock.
                self.initial_sftp_sweep_complete.set()
        else:
            # If SFTP is not enabled, we don't need to sweep, so we can proceed immediately.
            self.initial_sftp_sweep_complete.set()        
    # -----------------
    def stop(self):
        """
        Performs a robust, multi-stage shutdown of all pipeline services.
        """
        if not self.running.is_set():
            logging.warning("Shutdown already in progress.")
            return

        logging.info("="*60)
        logging.info("--- INITIATING GRACEFUL SHUTDOWN ---")
        logging.info("="*60)

        logging.info("Writing journal.")
        with self.status_lock:
            self.last_night_journal = self.generate_morning_journal()

         # --- PAUSE WATCHDOG DURING SHUTDOWN ---
        logging.warning("Pausing watchdog for final shutdown procedure.")
        self.watchdog_pause.set()
                                               
        # --- Phase 1: Stop all new data production ---
        logging.info("Setting global stop signal for all threads.")
        self.running.clear() # Global stop signal
        
        # --- Phase 2: Specifically stop the data production chain first ---
        self.stop_producer_thread() # This now handles both Capture and Detection

        # --- Phase 3: Signal all consumer and control threads to terminate ---
        logging.info("Posting final sentinels to all remaining worker and control queues...")
        queues_to_signal = [
            self.timelapse_q, self.event_q, self.event_stack_q,
            self.event_log_q, self.health_q, self.calibration_q
        ]
        if self.sftp_uploader:
            self.sftp_uploader.stop() # This handles the SFTP queues
        
        for q in queues_to_signal:
            try: q.put_nowait(None)
            except (queue.Full, AttributeError): pass
            
        # --- Phase 4: Wait for all remaining threads to finish cleanly ---
        logging.info("Waiting for all threads to complete their final tasks...")
        all_threads = self.worker_threads + self.control_threads
        shutdown_timeout = 10 # seconds

        for t in all_threads:
            if t == threading.current_thread() or t.name == "DashboardThread" or not t.is_alive():
                continue
            
            t.join(timeout=shutdown_timeout)
            if t.is_alive():
                logging.error(f"Thread '{t.name}' failed to join and is still alive. Shutdown may be incomplete.")

        # --- Phase 5: Release hardware resources ---
        if self.camera:
            try:
                self.camera.release()
                self.camera = None
            except Exception: pass
            
        if self.led_line:
            try:
                logging.info("Turning off status LED.")
                self.led_line.set_value(self.led_pin, gpiod.line.Value.INACTIVE)
                self.led_line.release()
            except Exception as e:
                logging.error(f"Error releasing LED GPIO pin: {e}")
        
        logging.info("="*60)
        logging.info("--- PIPELINE SHUTDOWN COMPLETE ---")
        logging.info("="*60)
    # -----------------
    def scheduler_loop(self):
        """
        A dedicated control thread that acts as the master timer. It starts and
        stops the producer thread based on the main schedule.
        """
        max_restart_attempts = self.cfg["general"].get("max_restart_failures", 3)
        logging.info("Master scheduler started. Will check every 60 seconds.")

        # --- Wait for the initial SFTP sweep to complete before doing anything. ---
        logging.info("Scheduler is waiting for initial SFTP sweep to complete...")
        self.initial_sftp_sweep_complete.wait()
        logging.info("Scheduler: SFTP sweep complete. Waiting for initial Janitor check...")
        self.initial_janitor_check_complete.wait()
        logging.info("Scheduler: All startup checks passed. Cleared to start normal operations.")
        
        logging.info("Scheduler: Waiting for 'System Ready' signal.")
        self.system_ready.wait()
        logging.info("Scheduler: Cleared to start normal operations.")
        
        while self.running.is_set():

            now_str = datetime.now().strftime("%H:%M")
            shutdown_time = self.cfg["general"].get("shutdown_time")

            # --- Scheduled System Shutdown Check ---
            if shutdown_time and now_str == shutdown_time:
                logging.warning(f"Scheduler: Reached scheduled shutdown time ({shutdown_time}).")
                # 1. Trigger video creation
                self.start_timelapse_video_creation()
                
                # 2. Wait for the video thread to finish (if it started)
                video_thread = next((t for t in threading.enumerate() if t.name == "TimelapseVideoThread"), None)
                
                if video_thread and video_thread.is_alive():
                    logging.info("Scheduler: Waiting for End-of-Session video creation to complete before shutdown...")
                    
                    # Use a loop with a timeout safety to prevents the shutdown from hanging forever if ffmpeg freezes
                    max_wait_loops = 600 # 600 * 1s = 10 minutes
                    for _ in range(max_wait_loops):
                        if not video_thread.is_alive():
                            logging.info("Scheduler: Video creation thread has finished.")
                            break
                        time.sleep(1)
                    else:
                        logging.error("Scheduler: Video creation timed out! Forcing shutdown anyway.")
                
                # 3. Perform Shutdown
                logging.info("Scheduler: Proceeding with system shutdown sequence.")
                time.sleep(2) # Final buffer for log flushing
                self.perform_system_action("shutdown", reason="Scheduled Daily Shutdown")
                break

            is_active_schedule = self.within_schedule()
            is_capturing = self.producer_thread_active()

            if self.emergency_stop_active.is_set():
                if is_capturing: self.stop_producer_thread()
                logging.warning("Scheduler: System is in EMERGENCY STOP due to an error. All capture is inhibited.")
                self.log_health_event("CRITICAL", "EMERGENCY_STOP", "Critical state.")
                # Wait for 60 seconds before checking again
                for _ in range(60):
                    if not self.running.is_set(): break
                    time.sleep(1)
                continue # Skip the rest of the scheduler logic   

            if self.camera_fatal_error.is_set():  # the camera reach max_camera_failures
                logging.error("Scheduler detected fatal camera error. Attempting restart (%d/%d)...", self.consecutive_restart_failures + 1, max_restart_attempts)
                self.log_health_event("ERROR", "CAMERA_DETECT_FAIL", "Fatal camera error.")
                self.stop_producer_thread() # Ensure it's fully stopped
                time.sleep(0.1)
                self.camera_fatal_error.clear() # Reset for new attempt
                self.start_producer_thread() # Attempt restart
                
                # Wait a moment to potentially fail again or stabilize
                time.sleep(10) 

                if not self.producer_thread_active():
                    self.consecutive_restart_failures += 1
                    logging.critical("Producer thread failed to restart. (%d/%d)",
                                     self.consecutive_restart_failures, max_restart_attempts)
                    if self.consecutive_restart_failures >= max_restart_attempts:
                        msg=f"!!! FAILED TO RECOVER AFTER {max_restart_attempts} ATTEMPTS. INITIATING SYSTEM REBOOT. !!!"

                        self.perform_system_action("reboot", reason=msg)
                        break
                else:
                    logging.info("Producer thread restarted successfully after fatal error.")
                    self.consecutive_restart_failures = 0 # Reset on success
            
            # --- START/STOP LOGIC ---
            if is_active_schedule and not is_capturing:
                if self.in_maintenance_mode.is_set():
                    # If we are in schedule but not capturing BECAUSE the user paused it,
                    # DO NOTHING. Log a message and let the maintenance timeout or user handle it.
                    logging.info("Scheduler: System is in scheduled active time but is currently in user-initiated Maintenance Mode.")
                elif self.is_calibrating.is_set():
                     # Also, do nothing if a calibration is running outside the schedule time
                     logging.info("Scheduler: System is in scheduled active time but is currently performing a calibration.")
                else:
                    logging.info("Scheduler: Active schedule window has begun. Starting producer.")
                    # --- BACKUP PREVIOUS MASTERPIECE ---
                    mip_path = os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.png")
                    if os.path.exists(mip_path):
                        # Use the start time of the PREVIOUS session for the filename
                        old_date_str = self.session_stats["start_time"].strftime("%Y%m%d")
                        archive_path = os.path.join(self.masterpiece_archive_dir, f"masterpiece_{old_date_str}.png")
                        try:
                            shutil.move(mip_path, archive_path)
                            logging.info(f"Backed up previous night's masterpiece to {archive_path}")
                        except Exception as e:
                            logging.error(f"Failed to backup masterpiece: {e}")

                    # --- CREATE FRESH CANVAS FOR NEW NIGHT ---
                    self.nightly_masterpiece = None
                    dummy = np.zeros((self.cfg["capture"]["height"], self.cfg["capture"]["width"], 3), dtype=np.uint16)
                    cv2.imwrite(os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.png"), dummy)
                    
                    self.session_stats = {k: (None if k=="start_time" else [] if isinstance(v, list) else 0) for k, v in self.session_stats.items()}
                    self.consecutive_restart_failures = 0
                    # --- Record session start time ---
                    self.session_start_time = datetime.now()
                    
                    self.capture_stable.clear()
                    self.session_stats = self._reset_session_stats()
                    
                    try:
                        self.watchdog_pause.set()
                        self.start_producer_thread()
                    finally:
                        self.watchdog_pause.clear()

                    logging.info("Scheduler: Requesting an immediate sky condition check.")
                    self.request_sky_analysis_trigger.set()

            elif not is_active_schedule and is_capturing and not self.is_calibrating.is_set():
                logging.info("Scheduler: Active schedule window has ended. Stopping producer.")
                self.consecutive_restart_failures = 0
                with self.status_lock:
                    self.last_night_journal = self.generate_morning_journal()
                self.stop_producer_thread()
                # --- Trigger end-of-session video creation ---
                self.start_timelapse_video_creation()
            
            elif not is_active_schedule and not is_capturing: 
                self.consecutive_restart_failures = 0
                if not self.in_maintenance_mode.is_set() and not self.is_calibrating.is_set():
                    logging.info("Scheduler: Outside the schedule time.")
                               
            # Wait for 60 seconds before the next check.
            # if self.running.wait(timeout=60):
                # break
            for _ in range(60):
                if not self.running.is_set(): break
                time.sleep(1)
    # -----------------
    def perform_system_action(self, action="exit", reason="Unknown reason"):
        """
        Unified handler for system-level state changes.
        Actions:
        - "exit": Exits script with status 1 (triggers systemd service restart).
        - "reboot": Gracefully stops pipeline, then reboots operating system.
        - "shutdown": Gracefully stops pipeline, then halts operating system.
        """
        logging.critical("="*60)
        logging.critical(f"!!! SYSTEM ACTION TRIGGERED: {action.upper()} !!!")
        logging.critical(f"!!! Reason: {reason} !!!")
        logging.critical("="*60)

        if action == "exit":
            logging.info("Exiting script with status 1 for service manager restart.")
            sys.exit(1)

        # For reboot/shutdown, we MUST try to stop gracefully first to save data.
        try:
            self.stop()
        except Exception as e:
            logging.error(f"Error during graceful stop before {action}: {e}")

        # Execute OS-level commands
        if action == "reboot":
            logging.info("Executing OS reboot...")
            subprocess.run(["sudo", "reboot"], check=False)
        elif action == "shutdown":
            logging.info("Executing OS halt...")
            subprocess.run(["sudo", "shutdown", "-h", "now"], check=False)
    # -----------------
    def generate_morning_journal(self):
        s = self.session_stats
        duration = datetime.now(timezone.utc) - s["start_time"]
        hours = duration.total_seconds() / 3600.0
        mean_lux = np.mean(s["avg_lux"]) if s["avg_lux"] else 0.0
        
        # Calculate derived metrics
        # eff = 100 - (s["rejected_contours"] / max(1, s["total_events"]) * 100)
        # total_obs_min = s["clear_sky_minutes"] + s["cloudy_sky_minutes"]
        filtered_noise = s["rejected_contours"]
        actual_captures = s["accepted_events"]
        total_pulsations = s["total_events"]
        
        clarity = (s["clear_sky_minutes"] / max(1, s["clear_sky_minutes"] + s["cloudy_sky_minutes"]) * 100)
        sky_avg = np.mean(s["avg_sky_score"]) if s["avg_sky_score"] else 0.0

        eff = 100.0
        if total_pulsations > 0:
            eff = 100 - (filtered_noise / total_pulsations * 100)        
        
        lines = [f"THE WATCHMAN'S JOURNAL | Night of {s['start_time'].strftime('%B %d, %Y')} Session Ending"]
        lines.append("-" * 60)
        lines.append(f"• SESSION: Vigilance kept for {hours:.1f} hours.")
        lines.append(f"• ENVIRONMENT: Average sky illuminance: {mean_lux:.4f} Lux.")
        lines.append(f"• THE SKY: Observed {clarity:.1f}% clarity with an average transparency of {sky_avg:.2f}.")
        
        if s["max_moon_impact"] > 0.1:
            lines.append(f"• LUNAR INFLUENCE: The Moon was in FOV for {s['moon_visible_minutes']:.2f}m (Max Impact: {int(s['max_moon_impact']*100)}%).")
        
        lines.append(f"• CAPTURES: Successfully isolated {s['accepted_events']} valid celestial events.")
        lines.append(f"• EFFICIENCY: Analyzed {total_pulsations:,} potential triggers; ignored {filtered_noise:,} noise artifacts ({eff:.1f}% accuracy).")

        lines.append(f"• DETECTIONS: Isolated {actual_captures} candidate celestial events.")
        
        lines.append(f"• STORAGE: Recorded {s["data_written_mb"]:.1f} MB of new astronomical data to disk.")
        
        # if self.sftp_uploader:
            # lines.append(f"• DATA SYNC: {s['sftp_success_count']} files successfully transmitted to the remote archive.")

        lines.append(f"• VITALS: Peak operating temperature: {s['peak_cpu_temp']:.1f}°C.")
        lines.append("-" * 60)
        lines.append("I am now resting. Monitoring the atmosphere for the next start.")

        # Save historical text file
        try:
            rpt_name = s["start_time"].strftime("journal_%Y%m%d.txt")
            with open(os.path.join(self.general_log_dir, rpt_name), 'w') as f:
                f.write("\n".join(lines))
        except (IOError, OSError) as e:
            logging.warning("Could not write journal file: %s", e)

        return "\n".join(lines)
    # -----------------
    # 3. THREAD MANAGEMENT
    # -----------------
    def start_producer_thread(self):
        """Starts ONLY the camera capture thread."""
        if any(t.name == "CaptureThread" for t in self.worker_threads if t.is_alive()):
            logging.warning("Capture thread appears to be already running.")
            return

        # Ensure the capture running flag is set before starting
        self.capture_running.set()

        capture_thread = threading.Thread(target=self.capture_loop, name="CaptureThread", daemon=True)
        detection_thread = threading.Thread(target=self.detection_loop, name="DetectionThread", daemon=True)

        capture_thread.start()
        detection_thread.start()

        self.worker_threads.append(capture_thread)
        self.worker_threads.append(detection_thread)

        logging.info("Data production pipeline started successfully.")
    # -----------------
    def stop_producer_thread(self):
        """
        Atomically and safely stops all data producer threads.
        Optimized V188: Handles long-exposures by breaking hardware locks and using dynamic timeouts.
        """
        producer_names_to_stop = ["CaptureThread", "DetectionThread"]
        threads_to_stop = [t for t in self.worker_threads if t.name in producer_names_to_stop and t.is_alive()]

        if not threads_to_stop:
            logging.warning("stop_producer_thread was called, but no producer threads were found running.")
            return

        # 1. Pause Watchdog
        try:
            logging.warning("Pausing watchdog for producer thread shutdown procedure.")
            self.watchdog_pause.set()
            self.update_worker_list(threads_to_stop)
        finally:
            logging.info("Resuming watchdog. Producer threads will now be joined.")
            self.watchdog_pause.clear()

        # 2. Signal Shutdown
        self.capture_running.clear()
        
        # 3. Force "Poison Pill" into Queue (Critical for DetectionThread)
        # If queue is full, drain one item to make room for the sentinel
        try:
            self.acq_q.put_nowait(None)
        except queue.Full:
            try:
                self.acq_q.get_nowait() # Discard old frame
                self.acq_q.put_nowait(None) # Insert stop signal
            except Exception: pass

        logging.info("Resetting background model for next session.")
        self.background = None

        # 4. Dynamic Timeout Calculation
        # Calculate safe timeout based on current exposure. 
        # Formula: (Exposure_Seconds * 2) + 10s Buffer. Min 15s.
        current_exp_sec = self.cfg["capture"].get("exposure_us") / 1000000
        safe_timeout = max(10.0, (current_exp_sec * 2.0) + 10.0)
        shutdown_deadline = time.time() + safe_timeout

        logging.info(f"Waiting for producer threads to terminate (Timeout: {safe_timeout:.1f}s)...")

        # 5. BREAK HARDWARE BLOCK (The Fix for Long Exposures)
        # If we are in long exposure, read() blocks. We force the camera to stop NOW.
        if self.camera and hasattr(self.camera, 'picam2'):
            try:
                # This breaks the blocking capture_array() call in the other thread
                logging.info("Sending hardware stop signal to interrupt exposure...")
                self.camera.picam2.stop()
            except Exception: 
                pass # Camera might already be stopped

        # 6. Join Threads
        for thread in threads_to_stop:
            while thread.is_alive():
                if time.time() > shutdown_deadline:
                    logging.critical(f"!!! FAILED TO STOP THREAD '{thread.name}' within the timeout. !!!")
                    msg = f"THREAD_JOIN_TIMEOUT: Thread {thread.name} refused to stop."
                    self.log_health_event("CRITICAL", "THREAD_JOIN_TIMEOUT", msg)
                    self.perform_system_action("reboot", reason=msg)
                    break 

                thread.join(timeout=0.5)
            
            if not thread.is_alive():
                logging.info(f"Thread '{thread.name}' has successfully terminated.")

        logging.info("Data production chain has been cleanly stopped.")
    # -----------------
    def update_worker_list(self, threads_to_stop):

        if not threads_to_stop:
            logging.warning("stop_producer_thread called but no producer threads were found running.")
            return

        # 2. Immediately remove these threads from the main worker list.
        #    This is the key step that prevents the watchdog race condition.
        self.worker_threads = [t for t in self.worker_threads if t not in threads_to_stop]
        logging.info(f"Removed {[t.name for t in threads_to_stop]} from watchdog monitoring list.")
    # -----------------
    def producer_thread_active(self):
        """Checks if the single CaptureThread is alive."""
        return any(t.name == "CaptureThread" and t.is_alive() for t in self.worker_threads)
    # -----------------
    # 4. CAMERA ACQUISITION & CONTROL
    # -----------------
    def capture_loop(self): 
        logging.info("Capture loop started. Waiting for the 'System Ready' signal...")
        
        # Wait until the main 'run' method signals that all threads are started.
        # This prevents this thread from producing data before consumers are ready.
        self.system_ready.wait()
        logging.info("'System Ready' signal received. Capture is now active.")

        first_run=True
        consecutive_failures = 0
        # idle_loop_counter = 0

        while self.running.is_set() and self.capture_running.is_set():
            
            max_failures = self.cfg["general"].get("max_camera_failures", 20)
            heartbeat_min = self.cfg["general"].get("idle_heartbeat_interval_min", 5)  
            
            if first_run:
                logging.info("Capture loop started. Max failures: %d. Heartbeat: %d min.", max_failures, heartbeat_min)
                first_run=False
                      
            # idle_loop_counter = 0
            ts = datetime.now(timezone.utc).isoformat()
            with self.camera_lock:
                frame = self.camera.read()

            if frame is None:
                # Use the new helper to track failures system-wide
                if not self._register_camera_failure():
                    return # Terminate this thread if fatal limit reached
                time.sleep(1) # Wait a bit longer after a single failure
                continue
            
            # If a frame is read successfully, reset the global counter
            self._reset_camera_failure()
            
            try:
                self.acq_q.put_nowait((ts, frame.copy()))
            except queue.Full:
                logging.warning("Acquisition queue full, dropping frame")
                self.log_health_event("WARNING", "ACQUISITION_QUEUE", "Acquisition queue full.")


        logging.info("Capture loop has exited.")
    # -----------------
    def update_operating_mode(self):
        """
        Checks camera's illuminance and dynamically reconfigures the camera
        for either 'Daylight Mode' (auto) or 'Night Mode' (manual) without
        stopping the capture thread.
        """
        lux_threshold = self.cfg.get("daylight", {}).get("lux_threshold", 10.0)
        # night_cfg = self.cfg["capture"]
        night_exposure_us = self.cfg["capture"].get("exposure_us")
        night_gain = self.cfg["capture"].get("gain")

        # Use a lock to ensure the capture_loop pauses while we change settings.
        with self.camera_lock:
            try:
                # 1. Temporarily switch to auto-exposure to get a good light reading
                self._set_camera_preset("DAY")
                # time.sleep(2) # Allow sensor to adjust
                frame = None
                metadata = {}
                
                while self.running.is_set():
                    frame = self.camera.read()
                    
                    if frame is None:
                        # Register failure. If it returns False, we reached max limit.
                        if not self._register_camera_failure():
                            try: self._set_camera_preset("NIGHT") 
                            except Exception as e:
                                logging.warning("Could not restore NIGHT preset after camera failure: %s", e)
                            return False # Abort entirely
                        time.sleep(1)
                        continue # Retry acquisition
                    
                    # Frame successfully acquired! Reset counter.
                    self._reset_camera_failure()
                    
                    try:
                        request = self.camera.picam2.capture_request()
                        metadata = request.get_metadata()
                        request.release()
                    except Exception as e:
                        logging.warning(f"Metadata extraction failed: {e}")
                    
                    break # Break out of the retry loop successfully

                if frame is None:
                    return False # Thread was stopped

                # 2. Capture the hardware's baseline choices
                hw_exp = metadata.get("ExposureTime", self.current_exposure_us)
                hw_gain = metadata.get("AnalogueGain", self.current_gain) 

                # 3. Analyze the actual brightness of the hardware's auto-frame
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                mean_bright = float(np.mean(gray)) / 256.0 
                
                # Save these for perform_auto_exposure() to use!
                self.ae_hw_exposure = hw_exp
                self.ae_hw_gain = hw_gain
                self.ae_hw_brightness = mean_bright
                
                # 4. Failsafe for missing Lux 
                if "Lux" not in metadata:
                    logging.warning("'Lux' missing from metadata. Estimating manually...")
                    # Rough fallback estimate if the sensor driver fails
                    current_lux = (mean_bright / (hw_exp/1e6 * hw_gain * 20.0)) if (hw_exp * hw_gain) > 0 else 0.0
                else:
                    current_lux = metadata["Lux"]

                with self.status_lock:
                    self.session_stats["avg_lux"].append(current_lux)
                    self.last_illuminance = f"{current_lux:.2f}"
                    
                _, moon_in_fov, _, _ = self.get_moon_state()
                
                logging.info(f"Light Check: {current_lux:.1f} Lux. HW Baseline: Exp={hw_exp}us, Gain={hw_gain:.2f}, Bright={mean_bright:.1f}")

                # --- CALCULATE CALIBRATION K ---
                # Formula: K = Brightness / (Lux * Exposure_sec * Gain)
                if frame is not None and current_lux > 0:
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    # median_brightness = np.median(gray)
                    mean_brightness = float(np.mean(gray)) # Use Mean!
                    
                    exposure_sec = hw_exp / 1_000_000.0
                    denominator = (current_lux * exposure_sec * hw_gain)
                    
                    if denominator > 0:
                        self.calibration_k = mean_brightness / denominator
                        self.camera_k_logger()
                
                logging.info(f"Light level check: Current illuminance is {current_lux:.1f} Lux). (k={self.calibration_k:.2f})")

                # Reconfigure camera for Day or Night
                if current_lux > lux_threshold:
                    # --- DAYLIGHT MODE ---
                    if not self.daylight_mode_active.is_set():
                        logging.warning("Illuminance is HIGH. Switching camera to DAYLIGHT MODE.")
                        self.daylight_mode_active.set()
                        self.base_timelapse_interval = self.cfg["timelapse"].get("daymode_interval_sec")
                        # self.timelapse_interval_sec = self.cfg["timelapse"].get("daymode_interval_sec")
                        self.timelapse_interval_sec = self.base_timelapse_interval

                else:
                    # --- NIGHT MODE ---
                    if self.daylight_mode_active.is_set():
                        logging.info("Illuminance is LOW. Switching camera to NIGHT MODE.")
                        self.daylight_mode_active.clear()
                        self.base_timelapse_interval = self.cfg["timelapse"].get("nightmode_interval_sec") 
                        # self.timelapse_interval_sec = self.cfg["timelapse"].get("nightmode_interval_sec")
                        self.timelapse_interval_sec = self.base_timelapse_interval
                        self.current_exposure_us = night_exposure_us
                        self.current_gain = night_gain

                    # self._set_camera_preset("NIGHT")

                return True

            except Exception:
                logging.error("Failed to set camera controls in update_operating_mode.", exc_info=True)
                # As a fallback, restore night settings to prevent being stuck in a bad state
                try:
                    self.current_exposure_us = night_exposure_us
                    self.current_gain = night_gain
                    self._set_camera_preset("NIGHT")
                except Exception as e:
                    logging.warning("Could not restore NIGHT preset after auto-exposure: %s", e)
                return False
    # -----------------
    def perform_auto_exposure(self):
        """
        Mathematical Auto-Exposure with Iterative Verification.
        Attempts to nail the exposure on the first try using Lux & K. 
        If the verification frame fails, it gracefully loops to correct and verify again.
        """
        logging.info("Performing mathematical auto-exposure with verification...")
        cfg = self.cfg["capture"]
        # --- SAVE SAFE FALLBACKS ---
        safe_night_exp = self.current_exposure_us
        safe_night_gain = self.current_gain

        # 1. Target calculation based on moon interference
        moon_impact, in_fov, _, _ = self.get_moon_state()
        base_target = cfg.get("target_sky_brightness", 15)
        target = base_target + (15 * moon_impact) if in_fov else base_target
        
        base_max = cfg.get("max_brightness", 240)
        effective_max = min(255, base_max + (40 * moon_impact if in_fov else 15 * moon_impact))
        
        tolerance = cfg.get("tolerance", 2)
        max_attempts = cfg.get("max_attempts", 3)

        min_gain, max_gain = cfg["min_gain"], cfg["max_gain"]
        min_exp, max_exp = cfg["min_exposure_us"], cfg["max_exposure_us"]

        # 2. Grab the Hardware Baseline captured milliseconds ago
        hw_exp = getattr(self, "ae_hw_exposure", self.current_exposure_us)
        hw_gain = getattr(self, "ae_hw_gain", self.current_gain)
        hw_bright = getattr(self, "ae_hw_brightness", 1.0)
        
        if hw_bright <= 0.5:
            hw_bright = 0.5 # Prevent division by zero

        # 3. Calculate Correction Ratio
        # How far off is the hardware's daylight auto-exposure from our night-sky target?
        ratio = target / hw_bright
        
        # Calculate the new required product (Exposure * Gain)
        required_product = (hw_exp * hw_gain) * ratio
        
        # 4. Distribute across Exposure and Gain
        if required_product > max_exp * min_gain:
            new_exposure = max_exp
            new_gain = required_product / max_exp
        else:
            new_exposure = required_product / min_gain
            new_gain = min_gain

        # Clamp initial math guess to hardware limits
        new_exposure = int(max(min_exp, min(max_exp, new_exposure)))
        new_gain = float(max(min_gain, min(max_gain, new_gain)))

        # 4. The Verification Loop
        for attempt in range(max_attempts):
            logging.info(f"Auto-exposure Attempt {attempt + 1}/{max_attempts} — Exp: {new_exposure}us | Gain: {new_gain:.2f}")

            with self.camera_lock:
                self._set_camera_preset("NIGHT", ExposureTime=new_exposure, AnalogueGain=new_gain)
                frame = self.camera.read()

            if frame is None:
                self._register_camera_failure() # <--- Track the failure
                logging.error("Auto-exposure: Frame capture failed during verification. Reverting to hardware baseline.")
                # self.current_exposure_us = int(hw_exp)
                # self.current_gain = float(hw_gain)
                self.current_exposure_us = safe_night_exp
                self.current_gain = safe_night_gain
                return
            
            self._reset_camera_failure() # <--- Reset on success

            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            # actual_brightness = float(np.median(gray))
            # actual_brightness = float(np.mean(gray))
            actual_brightness = float(np.mean(gray)) / 256.0

            logging.info(f"Auto-exposure Verification: Actual Brightness={actual_brightness:.1f} (Target={target:.1f} ±{tolerance})")

            # --- Check 1: Did we hit the target? ---
            if (target - tolerance) <= actual_brightness <= (target + tolerance):
                logging.info(f"Auto-exposure: Target accurately acquired on attempt {attempt + 1}.")
                self.current_exposure_us = new_exposure
                self.current_gain = new_gain
                return  # Success! Exit immediately.

            # --- Check 2: Calculate Correction for Next Attempt ---
            if attempt < max_attempts - 1:  # If we have more attempts left
                if actual_brightness > effective_max:
                    # SATURATED: Drastic failsafe reduction
                    logging.warning(f"Verification SATURATED ({actual_brightness:.1f}). Halving exposure.")
                    new_exposure = max(min_exp, new_exposure // 2)
                    if new_exposure == min_exp:
                        new_gain = max(min_gain, new_gain * 0.5)
                else:
                    # OFF-TARGET: Proportional ratio correction
                    correction_ratio = (target / actual_brightness) if actual_brightness > 0 else 2.0
                    
                    # if correction_ratio > 1.0: # Too dark -> Increase Exposure, then Gain
                        # if new_exposure < max_exp:
                            # new_exposure = int(min(max_exp, new_exposure * correction_ratio))
                        # else:
                            # new_gain = min(max_gain, new_gain * correction_ratio)
                    # else: # Too bright -> Decrease Gain, then Exposure
                        # if new_gain > min_gain:
                            # new_gain = max(min_gain, new_gain * correction_ratio)
                        # else:
                            # new_exposure = int(max(min_exp, new_exposure * correction_ratio))

                    # --- FIX: Treat Exposure and Gain as a single pooled product ---
                    current_product = new_exposure * new_gain
                    required_product = current_product * correction_ratio
                    
                    # Redistribute across Exposure and Gain correctly
                    if required_product > max_exp * min_gain:
                        new_exposure = max_exp
                        new_gain = required_product / max_exp
                    else:
                        new_exposure = required_product / min_gain
                        new_gain = min_gain

                # Re-clamp the corrected values before the next verification loop
                new_exposure = int(max(min_exp, min(max_exp, new_exposure)))
                new_gain = float(max(min_gain, min(max_gain, new_gain)))
            else:
                # We reached max_attempts without perfectly hitting the target
                logging.warning(f"Auto-exposure: Max attempts reached. Settling for Brightness={actual_brightness:.1f}")
                # self.current_exposure_us = new_exposure
                # self.current_gain = new_gain
                logging.warning(f"Auto-exposure: Max attempts reached without confirmation. Reverting to safe night settings (Exp={safe_night_exp}us, Gain={safe_night_gain:.2f}).")
                # self.current_exposure_us = int(old_night_exp)
                # self.current_gain = float(old_night_gain)
                self.current_exposure_us = safe_night_exp
                self.current_gain = safe_night_gain
    # -----------------
    def calculate_lux(self, brightness, exposure_us, gain):
        # Formula: Lux = Brightness / (K * Exposure_sec * Gain)
        k_val = self.calibration_k if (self.calibration_k and self.calibration_k > 0) else 20.0
        
        exposure_s = exposure_us / 1_000_000.0
        
        denominator = k_val * exposure_s * gain
        
        # 4. Safety Check (Prevent Division by Zero)
        if denominator <= 0.000001:
            return 0.0       

        lux = brightness / denominator
        return lux
    # -----------------
    def calibration_worker_loop(self):
        """
        A dedicated worker that waits for frames on the calibration_q,
        collects N of them, stacks them, and saves the result.
        """
        logging.info("Calibration worker started, waiting for triggered frames.")
        # calib_cfg = self.cfg.get("calibration", {})

        while self.running.is_set():
            try:
                # 1. Wait for the very first frame to arrive.
                first_frame_item = self.calibration_q.get(timeout=10)
                
                if first_frame_item is None:
                    # continue
                    break
                    
                stack_n = self.cfg["timelapse"].get("stack_N", 10)
                logging.warning("Calibration worker received first frame. Capturing %d total for test shot...", stack_n)
                
                with self.status_lock:
                    self.last_calibration_error = None

                frames_for_stack = [first_frame_item]
                capture_ok = True

                # 2. Collect the REMAINING frames.
                for i in range(stack_n - 1):
                    if not self.running.is_set():
                        capture_ok = False
                        break
                    try:
                        # Use a timeout in case the frame stream stops for any reason
                        item = self.calibration_q.get(timeout=10.0)
                        frames_for_stack.append(item)
                        logging.info(f"Captured live test frame {i+2}/{stack_n}...")
                    except queue.Empty:
                        error_msg = f"Timed out waiting for frame {i+2}/{stack_n}. Aborting test shot."
                        logging.error(f"Calibration worker: {error_msg}")
                        with self.status_lock:
                            self.last_calibration_error = error_msg
                        capture_ok = False
                        break
                
                # 3. Process the collected frames if everything went well.
                if capture_ok:
                    logging.info("Stacking %d frames for calibration image...", len(frames_for_stack))
                    stacked_image, _ = self.stack_frames(
                        [f for _, f in frames_for_stack],
                        align=self.cfg["timelapse"]["stack_align"],
                        method="mean",
                        min_features_for_alignment=self.cfg["timelapse"]["min_features_for_alignment"]
                    )
                    if stacked_image is not None:
                        tstamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"calibration_shot_{tstamp}.png"
                        full_path = os.path.join(self.calibration_out_dir, filename)
                        cv2.imwrite(full_path, stacked_image)
                        
                        with self.status_lock:
                            self.last_calibration_image_path = full_path
                        logging.info("Calibration image saved to: %s", full_path)
                    else:
                        raise RuntimeError("Frame stacking returned None.")
                
            except queue.Empty:
                # This is normal, it just means no trigger was received in the last 10s.
                continue
            except Exception as e:
                error_msg = f"An unexpected error occurred during test shot: {e}"
                logging.exception(error_msg)
                with self.status_lock:
                    self.last_calibration_error = error_msg
            finally:
                # 4. CRITICAL: Always reset the state for the next run.
                # Clear the trigger so the detection_loop stops sending frames.
                # Drain any extra frames that might have queued up during stacking.
                if self.is_calibrating.is_set():
                    logging.warning("Calibration worker is terminating the temporary capture session.")
                    self.stop_producer_thread()
                    self.is_calibrating.clear() # Take system out of calibration mode

                while not self.calibration_q.empty():
                    try:
                        self.calibration_q.get_nowait()
                    except queue.Empty:
                        break
    # -----------------
    def _set_camera_preset(self, mode="NIGHT", **overrides):
        """
        Central authority for camera hardware.
        Modes: 'NIGHT' (Manual Hunt), 'DAY' (Auto), 'SNAP' (Long Exp Snapshot)
        """
        # 1. Define the base templates using current config/state
        presets = {
            "NIGHT": {
                "ExposureTime": int(self.current_exposure_us),
                "AnalogueGain": float(self.current_gain),
                "AeEnable": False,
                "AwbEnable": False,
                "FrameDurationLimits": (int(self.current_exposure_us), int(self.current_exposure_us)),
                "ColourGains": [self.cfg["capture"].get("red_gain", 2.0), self.cfg["capture"].get("blue_gain", 1.5)]
            },
            "DAY": {
                "AeEnable": True, 
                "AwbEnable": True,
                "FrameDurationLimits": (1000, 33333) 
            },
            "SNAP": {
                "ExposureTime": int(self.cfg["capture"].get("snap_exposure_us")),
                "AnalogueGain": float(self.cfg["capture"].get("snap_gain", 4.0)),
                "AeEnable": False,
                "AwbEnable": False,
                "FrameDurationLimits": (int(self.cfg["capture"].get("snap_exposure_us")), int(self.cfg["capture"].get("snap_exposure_us"))),
                "ColourGains": [self.cfg["capture"].get("red_gain", 2.0), self.cfg["capture"].get("blue_gain", 1.5)]
            }
        }

        # 2. Merge preset with any dynamic overrides (like from Auto Exposure loop)
        controls = {**presets.get(mode, presets["NIGHT"]), **overrides}
        
#        with self.camera_lock:
        try:
            if mode != "SNAP":
                # 3. Synchronize internal state variables if they were changed
                if "ExposureTime" in controls:
                    self.current_exposure_us = controls["ExposureTime"]
                if "AnalogueGain" in controls:
                    self.current_gain = controls["AnalogueGain"]

            # Apply to hardware
            self.camera.picam2.set_controls(controls)

            applied_exp_us = controls.get("ExposureTime", self.current_exposure_us)
            applied_gain = controls.get("AnalogueGain", self.current_gain)

            # --- THE METADATA FLUSH ---
            # If we are in DAY mode, in controls there aren't applied_exp_us and applied_gain so the default are used
            # is automatically choosing it. So we just flush 4 frames quickly.
            if mode == "DAY":
                self.camera.flush() # Uses the fallback 4-frame loop
                logging.info("Preset: Mode DAY applied. (Auto-Exposure Unlocked)")
            else:
                # --- HYBRID SYNC: Part 1 ---
                # Wait for the physical exposure time to finish integrating light BEFORE 
                # we start aggressively pulling frames from the buffer.
                settle_time = (applied_exp_us / 1000000.0) + 0.1 
                time.sleep(settle_time)
                
                # --- HYBRID SYNC: Part 2 ---
                # Now that the light is collected, check the metadata to clear stale frames.
                # For NIGHT and SNAP, we tell the flush method exactly what Exposure 
                # to look for. It will pull frames natively until the hardware syncs.
                self.camera.flush(target_exposure_us=applied_exp_us, target_gain=applied_gain)
                logging.info(f"Preset: Mode {mode} applied. (Exp: {applied_exp_us}us, Gain: {applied_gain:.2f})")

            # self.camera.flush(target_exposure_us=applied_exp_us, target_gain=applied_gain)
            
            # logging.info(f"Preset: Mode {mode} applied. (Exp: {applied_exp_us}us, Gain: {applied_gain:.2f})")
            # logging.info(f"Preset: Mode {mode} applied. (Exp: {applied_exp_us}us, Gain: {controls.get('AnalogueGain', self.current_gain):.2f})")
            return True
        except Exception as e:
            logging.error(f"Preset: Failed to apply preset {mode}: {e}")
            return False
    # -----------------
    def _load_calibration_frames(self):
        """
        Loads Dark, Bias, and Flat frames. 
        Pre-calculates the Flat Gain Map for fast runtime application.
        """
        cfg = self.cfg["detection"]
        
        # 1. Load Master Dark (Standard subtraction)
        if cfg["dark_frame_path"] and os.path.exists(cfg["dark_frame_path"]):
            self.master_dark = cv2.imread(cfg["dark_frame_path"], cv2.IMREAD_UNCHANGED)
            logging.info(f"Loaded Master Dark: {cfg['dark_frame_path']}")
        else:
            self.master_dark = None

        # 2. Load Bias (Needed to calibrate the Flat)
        bias_frame = None
        if cfg["bias_frame_path"] and os.path.exists(cfg["bias_frame_path"]):
            bias_frame = cv2.imread(cfg["bias_frame_path"], cv2.IMREAD_UNCHANGED).astype(np.float32)
            logging.info(f"Loaded Master Bias: {cfg['bias_frame_path']}")

        # 3. Load and Process Flat Frame
        self.flat_multiplier = None
        if cfg["flat_frame_path"] and os.path.exists(cfg["flat_frame_path"]):
            try:
                raw_flat = cv2.imread(cfg["flat_frame_path"], cv2.IMREAD_UNCHANGED).astype(np.float32)
                logging.info(f"Loaded Master Flat: {cfg['flat_frame_path']}")

                # A. Subtract Bias from Flat (Crucial for mathematical accuracy)
                if bias_frame is not None:
                    # Ensure dimensions match
                    if raw_flat.shape == bias_frame.shape:
                        raw_flat = cv2.subtract(raw_flat, bias_frame)
                        # Clip to avoid negative values or divide by zero later
                        raw_flat = np.maximum(raw_flat, 1.0) 
                    else:
                        logging.warning("Bias and Flat dimensions mismatch! Skipping Bias subtraction on Flat.")

                # B. Normalize: GainMap = Mean / PixelValue
                # This creates a map where dark corners have values > 1.0 (to boost brightness)
                # and bright centers have values < 1.0
                
                # Calculate mean per channel for color accuracy
                flat_mean = np.mean(raw_flat, axis=(0,1)) # Result is [B_mean, G_mean, R_mean]
                
                # Create the multiplier map
                self.flat_multiplier = np.divide(flat_mean, raw_flat)
                
                logging.info("Flat Field Gain Map generated successfully.")
                
            except Exception as e:
                logging.error(f"Failed to process Flat Frame: {e}")
                self.flat_multiplier = None
    # -----------------
    def camera_k_logger(self):
        if not os.path.exists(self.camera_k_file):
            with open(self.camera_k_file, "w") as f:
                f.write("timestamp, k\n")
                
        try:
            # ts = datetime.now(timezone.utc).isoformat()
            line = (f"{datetime.now(timezone.utc).isoformat()},{self.calibration_k}\n")

            with open(self.camera_k_file, "a") as f:
                f.write(line) 
        except Exception as e:
            print(f"[camera_k_logger] Failed to write camera k value: {e}")
    # -----------------
    # 5. ANALYSIS & DETECTION
    # -----------------
    def _get_dynamic_accumulate_alpha(self):
        """
        Calculates the ideal accumulate_alpha based on the CURRENT exposure time.
        Logic: We want the background to adapt to changes in approximately 20 seconds.
        
        Short Exposure (0.04s) -> Needs tiny alpha (0.002) to smooth noise.
        Long Exposure (4.0s)   -> Needs big alpha (0.2) to prevent ghosting/halos.
        """
        # How many seconds should it take for a change to become background?
        target_adaptation_time = 20.0 
        
        if not self.current_exposure_us:
            exp_seconds = 0.1
        else:
            exp_seconds = self.current_exposure_us / 1_000_000.0
            
        # Calculate raw alpha = actual exposure (sec) / target_time
        alpha = exp_seconds / target_adaptation_time
        
        # CLAMP limits to prevent breaking the background model
        # 0.001 = Extremely slow update (good for high FPS video)
        # 0.300 = Fast update (good for very long exposures to kill moon ghosts)
        return max(0.001, min(0.3, alpha))
    # -----------------
    def detection_loop(self):
        logging.info("Detection loop started.")
        
        # Rate limiter for debug saves
        last_debug_save = 0
        debug_save_interval = 2.0 # Seconds
        
        buffer_frames = self.cfg["detection"].get("pre_event_frames", 8)
        cooldown_frames = self.cfg["detection"].get("event_cooldown_frames", 16)
        recent_frames = deque(maxlen=buffer_frames)
        
        # --- State Management for creating two different packages ---
#        in_event = False
        current_event_video_frames = []
        current_event_stack_frames = []
        event_cooldown_counter = 0

        # Forcibly end any event that lasts longer than this many frames to prevent memory leaks.
        # A real meteor will never last this long.
        max_event_frames = self.cfg["detection"].get("max_event_frames", 300)
        
        current_event_frame_count = 0
        
        if not hasattr(self, "background_reset_time"):
            self.background_reset_time = 0

        while self.running.is_set():
            # Wait for the sky monitor to complete its first check.
            if not self.capture_stable.is_set():
                try:
                    # 1. Drain the Acquisition Queue (Filled by CaptureThread)
                    self.acq_q.get(timeout=1.0)
                    
                    # 2. Drain the Stale Timelapse Queue (Stale frames from before the check)
                    while not self.timelapse_q.empty():
                        try:
                            self.timelapse_q.get_nowait()
                        except queue.Empty:
                            break
                    
                    logging.info("Detection loop is discarding pre-check frames...")
                except queue.Empty:
                    # If the queue is empty, just wait patiently.
                    time.sleep(0.5)
                continue # Loop back and check the flag again.
            
            try:
                item = self.acq_q.get()
                if item is None:
                    logging.info("Detection loop received sentinel. Draining complete, exiting.")
                    break
                ts, frame = item
            except queue.Empty:
                continue

            new_buffer_size = self.cfg["detection"].get("pre_event_frames", 8)
            cooldown_frames = self.cfg["detection"].get("event_cooldown_frames", 16)
            if recent_frames.maxlen != new_buffer_size:
                logging.info(f"Updating pre-event buffer from {recent_frames.maxlen} to {new_buffer_size} frames.")
                recent_frames = deque(list(recent_frames), maxlen=new_buffer_size)

            # 1. Dark Subtraction (Integer Math - Fast)
            if self.master_dark is not None:
                # frame = cv2.subtract(frame, self.master_dark)
                frame = cv2.subtract(frame, self.master_dark.astype(np.uint16))

            # 2. Flat Field Correction (Float Math - Slower but necessary)
            if self.flat_multiplier is not None:
                try:
                    # Convert to float, multiply by gain map, clip, convert back
                    frame_f = frame.astype(np.float32)
                    frame_f = cv2.multiply(frame_f, self.flat_multiplier)
                    # frame = np.clip(frame_f, 0, 255).astype(np.uint8)
                    frame = np.clip(frame_f, 0, 65535).astype(np.uint16)
                except Exception:
                    pass # Fallback to uncorrected if dimension mismatch occurs

            # 3. Calibration Queue
            if self.is_calibrating.is_set():
                try:
                    self.calibration_q.put_nowait((ts, frame.copy()))
                except queue.Full:
                    logging.warning("Calibration queue is full.")
                continue

            with self.status_lock:
                effective_threshold = self.effective_threshold

            # already should have a value...
            if effective_threshold is None:
                effective_threshold = self.cfg["detection"]["base_threshold"]
                
            if not isinstance(effective_threshold, (int, float)):
                logging.warning("Invalid effective_threshold (%r), falling back to base_threshold", effective_threshold)
                effective_threshold = self.cfg["detection"]["base_threshold"]
                
#            logging.info(f"effective_threshold value = {effective_threshold}")

            # --- DEBUG REPORTING ---
            debug_level = self.cfg["general"].get("debug_level", 0)

            # 2. Now, check if we should even attempt the meteor hunt. 
                        
            if self.daylight_mode_active.is_set() or self.weather_hold_active.is_set():
                threshold_label = "N/A (Paused)"
                
                # --- NEW: Smart Auto-Throttling (Day/Hold) ---
                self._enqueue_timelapse_frame(ts, frame, threshold_label)

                if debug_level >= 1:
                    debug_img = None
                    debug_payload = {"Daylight_mode": self.daylight_mode_active.is_set(), "weather_hold_active": self.weather_hold_active.is_set()}
                    self.publish_debug("detection_stats", debug_payload, image=debug_img)
                continue
                
				# # Still pass frame to timelapse with the status string
                # try: 
                    # self.timelapse_q.put_nowait((ts, frame, threshold_label))
                    # if debug_level >= 1:
                        # debug_img = None
                        # debug_payload = {"Daylight_mode": self.daylight_mode_active.is_set(), "weather_hold_active": self.weather_hold_active.is_set()}
                        # self.publish_debug("detection_stats", debug_payload, image=debug_img)
                # except queue.Full: 
                    # logging.warning("Timelapse queue is full.")
                # continue               

            # --- Detection Logic ---
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray = cv2.GaussianBlur(gray, (5,5), 0)

            current_alpha = self._get_dynamic_accumulate_alpha()
            # If SkyMonitor sets self.background = None mid-loop, 'local_bg' keeps the image object alive.
            local_bg = self.background 
                           
            if local_bg is None:
                # Initialize both local and shared state
                self.background = gray.astype("float32")
                #recent_frames.append((ts, frame.copy(), effective_threshold))
                continue
         
            # Cooldown after background reset (prevents false events)
            if time.time() - self.background_reset_time < 5:
                # Allow background to stabilize, but do NOT trigger events
                cv2.accumulateWeighted(gray.astype(np.float32), local_bg, current_alpha)
                continue
            
            # Only update the background model if we are NOT in an event.
            if not self.event_in_progress.is_set():
                # cv2.accumulateWeighted(gray, self.background, self.cfg["detection"]["accumulate_alpha"])
                cv2.accumulateWeighted(gray.astype(np.float32), local_bg, current_alpha)
                 
            # --- AUTO-THROTTLE ---
            self._enqueue_timelapse_frame(ts, frame, effective_threshold)                   

            # bg = cv2.convertScaleAbs(self.background)
            # bg = cv2.convertScaleAbs(local_bg)
            # bg = np.clip(local_bg, 0, 65535).astype(np.uint16)
            bg = local_bg
            # diff = cv2.absdiff(gray, bg)
            diff = cv2.absdiff(gray.astype(np.float32), bg)

            # Reject tiny / noise-only changes (pre-filter)
            # | Resolution | Suggested value |
            # | ---------- | --------------- |
            # | 640×480    | 10–20           |
            # | 1280×720   | 20–40           |
            # | 1920×1080  | 40–80           |
                       
            # if self.cfg["capture"]["width"] == 640: min_changes = 10
            # elif self.cfg["capture"]["width"] == 1280: min_changes = 20
            # elif self.cfg["capture"]["width"] == 1920: min_changes = 40
            # else: min_changes = 30

            # # Scale min_changes based on total megapixel count (Dynamic)
            # megapixels = (self.cfg["capture"]["width"] * self.cfg["capture"]["height"]) / 1_000_000
            # min_changes = int(megapixels * 25) # Approx 23 for 720p, 52 for 1080p, 300 for HQ
            
            # _, th = cv2.threshold(diff, int(effective_threshold), 255, cv2.THRESH_BINARY)
            # motion_pixels = cv2.countNonZero(th)
            # #if motion_pixels < min_changes:
            # if motion_pixels < self.min_changes_required:
                # continue

            # thr16 = int(effective_threshold) * 256
            thr16 = int(effective_threshold) * 4
            motion_pixels = cv2.countNonZero((diff > thr16).astype("uint8"))
            if motion_pixels < self.min_changes_required:
                continue

            # _, th = cv2.threshold(diff.astype(np.float32), thr16, 255, cv2.THRESH_BINARY)
            _, th = cv2.threshold(diff, thr16, 255, cv2.THRESH_BINARY)
            th = th.astype(np.uint8)


            # --- Use the new effective_threshold in the OpenCV call ---
            # th = cv2.morphologyEx(th, cv2.MORPH_OPEN, np.ones((3,3), np.uint8))
            th = cv2.morphologyEx(th, cv2.MORPH_CLOSE, np.ones((3,3), np.uint8))
                       
            # Finds the contours and creates the 'cnts' variable.
            cnts, _ = cv2.findContours(th, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
          								
            # min_area = self.cfg["detection"].get("min_area", 50)
            # detected = any(cv2.contourArea(c) >= min_area for c in cnts)
            
            accepted_cnts = 0
            rejection_stats = {"area": 0, "total": len(cnts)}
            raw_contour_count = len(cnts)
            
            for c in cnts:
                area = cv2.contourArea(c) 
                # if area < min_area:
                if area < self.min_area:
                    rejection_stats["area"] += 1
                    with self.status_lock:
                        self.session_stats["rejected_contours"] += 1
                else:
                    accepted_cnts += 1
            
            detected = (accepted_cnts > 0)
             
            if debug_level >= 1:
                mean_val, std_dev = cv2.meanStdDev(diff)
                # noise_sigma = float(std_dev[0][0]) / 256.0
                noise_sigma = float(std_dev[0][0]) / 4.0
                # Payload construction
                debug_payload = {"noise_sigma": round(noise_sigma, 2), "threshold": effective_threshold, "contours_total": len(cnts), "contours_accepted": accepted_cnts, "rejected": rejection_stats, "event_active": self.event_in_progress.is_set()}
                
                # If Level >= 3, prepare the visualization image
                debug_img = None
                if debug_level >= 3:
                    # Create Quad View
                    h, w = gray.shape
                    s = 0.5
                    new_size = (int(w * s), int(h * s))

                    def ensure_color(img):
                        # If image is 2D (H, W), convert to 3D (H, W, 3)
                        if len(img.shape) == 2:
                            return cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
                        return img

                    # d1: Original Gray, d2: Diff, d3: Thresh, d4: Frame with Contours
                    d1 = cv2.resize(ensure_color(gray), new_size)
                    d2 = cv2.resize(ensure_color(diff), new_size)
                    d3 = cv2.resize(ensure_color(th), new_size)
                    d4 = cv2.resize(ensure_color(frame), new_size)
                    
                    cv2.drawContours(d4, cnts, -1, (0, 0, 65535), 1)
                    
                    # Stack safely
                    top = np.hstack((d1, d2))
                    bot = np.hstack((d3, d4))
                    # debug_img = np.vstack((top, bot))
                    debug_img = (np.vstack((top, bot)) >> 8).astype(np.uint8)
                    
                    # Disk Save (Level 2+) for Event Analysis
                    if debug_level == 2 and self.event_in_progress.is_set():
                        now = time.time()
                        if now - last_debug_save > debug_save_interval:
                            debug_dir = os.path.join(self.general_log_dir, "debug_detection")
                            os.makedirs(debug_dir, exist_ok=True)
                            fname = os.path.join(debug_dir, f"det_{datetime.now().strftime('%H%M%S_%f')}.jpg")
                            cv2.imwrite(fname, debug_img)
                            last_debug_save = now

                # Send via ZMQ
                self.publish_debug("detection_stats", debug_payload, image=debug_img)

            # The pre-event buffer always contains the most recent frames.
            recent_frames.append((ts, frame.copy(), effective_threshold))
           
            if detected:
                if not self.event_in_progress.is_set():
                    # --- EVENT START ---
                    logging.info("New event started at %s (contours=%d)", ts, len(cnts))
                    self.event_in_progress.set()
                    # The video package starts with the complete pre-event buffer.
                    current_event_video_frames = list(recent_frames)
                    # The stack package starts clean, with ONLY the first detected frame.
                    current_event_stack_frames = [(ts, frame.copy(), effective_threshold)]
                    current_event_frame_count = len(current_event_video_frames)
                    with self.status_lock:
                        # Track noise: all rejected contours from this event
                        self.session_stats["rejected_contours"] += rejection_stats["total"]
                        self.session_stats["accepted_events"] += 1
                else:
                    # --- EVENT CONTINUES ---
                    # Add the current frame to both packages.
                    current_event_video_frames.append((ts, frame.copy(), effective_threshold))
                    current_event_stack_frames.append((ts, frame.copy(), effective_threshold))
                    current_event_frame_count += 1
                
                event_cooldown_counter = 0 # Reset cooldown on new detection
                with self.status_lock:
                    self.session_stats["total_events"] += raw_contour_count
            
            elif self.event_in_progress.is_set():
                # --- EVENT COOLDOWN ---
                event_cooldown_counter += 1
                # Add cooldown frames to the video for context.
                current_event_video_frames.append((ts, frame.copy(), effective_threshold))
                current_event_frame_count += 1
            
#            logging.info(f"ecc {event_cooldown_counter} su {cooldown_frames} - cef {current_event_frame_count} su {max_event_frames}")
            if self.event_in_progress.is_set() and (event_cooldown_counter > cooldown_frames or current_event_frame_count >= max_event_frames):

                if current_event_frame_count >= max_event_frames:
                    # --- EVENT END ---
                    logging.error(f"EVENT TIMEOUT: Event forcibly terminated after {current_event_frame_count} frames to prevent memory leak.")
                    self.log_health_event("ERROR", "EVENT_TIMEOUT", f"Forced event termination after {current_event_frame_count} frames.")

                logging.info("Event finished. Dispatching packages (Video: %d frames, Stack: %d frames).", len(current_event_video_frames), len(current_event_stack_frames))

                # --- START DIVINE MASTERPIECE LOGIC ---
                # --- Celestial Accumulation Map ---
                if len(current_event_stack_frames) > 0:
                    try:
                        # Extract just the frame data from the tuple (ts, frame, thr)
                        frames_for_mip = [f[1] for f in current_event_stack_frames]
                        
                        # Use high-speed MIP (Maximum Intensity Projection)
                        event_mip = np.maximum.reduce(frames_for_mip)

                        if self.nightly_masterpiece is None:
                            self.nightly_masterpiece = event_mip.copy()
                        else:
                            # Keep the brightest parts (Meteor path + background stars)
                            self.nightly_masterpiece = np.maximum(self.nightly_masterpiece, event_mip)
                        
                        # Atomic save to ensure no partially written images appear on dashboard
                        mip_path = os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.png")
                        temp_mip_path = mip_path + ".tmp.png"
                        
                        cv2.imwrite(temp_mip_path, self.nightly_masterpiece)
                        os.replace(temp_mip_path, mip_path)
                        logging.debug("Nightly Masterpiece updated with new cosmic path.")

                    except Exception as e:
                        logging.error(f"Failed to update Masterpiece: {e}")
                # --- END DIVINE MASTERPIECE LOGIC ---

                try:
                    # Send the two specialized packages to their consumers.
                    self.event_q.put_nowait(current_event_video_frames)
                    self.event_stack_q.put_nowait(current_event_stack_frames)
                    
                    # Create and dispatch the summary dictionary for the event logger
                    if self.cfg.get("event_log", {}).get("enabled", True):
                        start_dt = datetime.fromisoformat(current_event_video_frames[0][0])
                        end_dt = datetime.fromisoformat(current_event_video_frames[-1][0])
                        log_summary = {
                            "timestamp_utc": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                            "start_time_utc": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "duration_sec": (end_dt - start_dt).total_seconds(),
                            "num_frames_video": len(current_event_video_frames), 
                            "num_frames_stack": len(current_event_stack_frames)
                        }
                        self.event_log_q.put_nowait(log_summary)
                  
                except queue.Full:
                    logging.warning("An event queue was full. Data for this event was dropped.")

                # # --- Celestial Accumulation Map ---
                # event_max_projection = np.max([f for (_, f, _) in current_event_stack_frames], axis=0).astype(np.uint8)
                
                # if self.nightly_masterpiece is None:
                    # self.nightly_masterpiece = event_max_projection
                # else:
                    # # Keep the brightest pixels from either the masterpiece or the new event
                    # self.nightly_masterpiece = cv2.max(self.nightly_masterpiece, event_max_projection)
                
                # # Save the masterpiece to disk so it survives a crash
                # cv2.imwrite(os.path.join(self.daylight_out_dir, "nightly_masterpiece_live.jpg"), self.nightly_masterpiece)
                
                # Reset state for the next event.
                self.event_in_progress.clear()
                current_event_video_frames = []
                current_event_stack_frames = []
                event_cooldown_counter = 0
 #               self.background = None
                current_event_frame_count = 0
    # -----------------
    def sky_monitor_loop(self):
        logging.info("Sky monitor started.")
        
        while self.running.is_set():
            check_min = self.cfg["daylight"].get("check_interval_min")
      
            # 1. Only perform a check if the main capture thread is currently active.
            if self.producer_thread_active():
                sky_state_changed = False
                
                logging.info("Sky Monitor: Routine check starting. Pausing data acquisition.")
                self.capture_stable.clear()
                # A. Check for daylight to determine the primary operating mode.
                self.update_operating_mode()
      
                # Only strictly synchronize if in Night Mode. 
                # In Day Mode, we prioritize the check over timelapse continuity.
                if not self.daylight_mode_active.is_set() and self.capture_stable.is_set():
                    if not self.wait_for_safe_window():
                        continue

                # B. If we are in Night Mode, also check for clouds.
                if not self.daylight_mode_active.is_set():
                    if self.cfg["capture"].get("auto_exposure_tuning"):
                        self.perform_auto_exposure()

                    status, result = self.analyze_celestial_context()
                    # if status is not Error, result is a dictionary with {"stddev": raw_stddev, "norm_stddev": norm_stddev, "mean":roi_mean, "stars": star_count}
                    
                    sky_state_changed = False
                    # Sky condition change
                    # if status != SkyConditionStatus.ERROR and status != self.last_sky_status:
                    if status != self.last_sky_status:
                        logging.info(f"Sky Monitor: Sky state changed from {self.last_sky_status} to {status}")
                        sky_state_changed = True
                        self.last_sky_status = status

                    # Day / Night mode change
                    # current_daylight_mode = self.daylight_mode_active.is_set()
                    if self.daylight_mode_active.is_set() != self.last_daylight_mode:
                        logging.info(f"Sky Monitor: Daylight mode changed from {self.last_daylight_mode} to {self.daylight_mode_active.is_set()}"
                        )
                        sky_state_changed = True
                        self.last_daylight_mode = self.daylight_mode_active.is_set()

                    if status == SkyConditionStatus.CLEAR and self.weather_hold_active.is_set():
                        logging.info("Sky Monitor: Sky has cleared. Releasing WEATHER HOLD.")
                        self.weather_hold_active.clear()
                    
                    elif status == SkyConditionStatus.CLOUDY and not self.weather_hold_active.is_set():
                        logging.warning("Sky Monitor: Clouds or poor conditions detected. Engaging WEATHER HOLD.")
                        self.weather_hold_active.set()
                    
                    elif status == SkyConditionStatus.ERROR:
                        logging.error("Sky Monitor: Failed to determine sky conditions due to an error.")
                        logging.info(f"Sky Monitor: Error {result}")
                        self.log_health_event("ERROR", "SKY_MONITOR_FAIL", "Failed to determine sky conditions due to an error.")
                        self.weather_hold_active.set()
                        
                        with self.status_lock:
                            self.effective_threshold = self.threshold_max
                            self.last_effective_threshold = f"{self.threshold_max} (FAILSAFE)"
                            
                # Reset the background model to prevent false triggers.
                                                                                  
                if sky_state_changed:
                    logging.info("Sky Monitor: Background reset due to state change.")
                    self.background = None
                    self.background_reset_time = time.time()

                # Signal that the initial check is complete.
                if not self.capture_stable.is_set():
                    logging.info("Sky Monitor: Initial check complete. Releasing detection thread.")
                    self.capture_stable.set()
            
            # 3. Wait for the next full interval before checking again.
            logging.info("Sky Monitor: Next check in %d minutes or upon request.", check_min)
            for _ in range(check_min * 60):
                if not self.running.is_set(): break # Allow for fast shutdown
                if self.request_sky_analysis_trigger.is_set():
                    logging.info("Sky Monitor: Immediate check requested. Aborting wait and running now.")
                    self.request_sky_analysis_trigger.clear()
                    break 
                time.sleep(1)
    # -----------------
    def analyze_celestial_context(self) -> Tuple[SkyConditionStatus, dict]:
        """
        Captures a snapshot, analyzes it, and returns a detailed status.
        """
        logging.info("Capturing sky condition snapshot...")
        night_cfg = self.cfg["capture"]
        cloudy_interval_time = 60
        
        now_ts = time.time()
        # Calculate elapsed time since dusk (if 1st check) or since last check
        elapsed_seconds = now_ts - self.session_stats["last_check_time"]
        elapsed_minutes = elapsed_seconds / 60.0

        with self.camera_lock:        
            try:     
                snap_exposure_us = self.cfg["capture"].get("snap_exposure_us")
                snap_gain = self.cfg["capture"].get("snap_gain")
                # Auto-exposure limits shutter speed based on framerate (usually 33ms).
                # This is too dark for star analysis. We force snap_exposure_us.
                logging.info(f"Snapshot Mode: NIGHT (Manual Long Exposure {snap_exposure_us:.1f}us)")
                self._set_camera_preset("SNAP")
                # Wait for exposure + readout (10s exposure needs >10s wait)
                # wait_time = (snap_exposure_us / 1000000.0) + 1.0
                # logging.info(f"Waiting {wait_time:.1f}s for long exposure integration...")
                # time.sleep(wait_time)

                frame = self.camera.read()
                if frame is None:
                    # Register the failure globally!
                    self._register_camera_failure()
                    logging.error("Failed to capture sky condition snapshot frame.")
                    self.log_health_event("ERROR", "CAPTURE_DAYLIGHT_FAIL", "Failed to capture sky condition snapshot frame.")
                    return SkyConditionStatus.ERROR, {"message": "Camera read returned None."}
                
                # Success!
                self._reset_camera_failure()
                
                # --- Analysis and Metadata Saving ---
                today = datetime.now().strftime("%Y-%m-%d_%H-%M")
                results = self.analyze_sky_conditions(frame)
                
                full_path = os.path.join(self.daylight_out_dir, f"sky_conditions_{today}.png")
                cv2.imwrite(full_path, frame)
                self._tally_data_write(full_path)
                
                moon_impact, moon_in_fov, moon_alt, moon_dist = self.get_moon_state()
                
                with self.status_lock:
                    self.last_sky_conditions = results
                    self.last_moon_status = {
                        "impact": moon_impact,
                        "in_fov": moon_in_fov,
                        "alt": moon_alt,
                        "dist": moon_dist
                    }
                
                self.session_stats["max_moon_impact"] = max(self.session_stats["max_moon_impact"], moon_impact)
                moon_penalty = 0.0 
                # === Adaptive threshold update (REFACTORED) ===
                if self.cfg["detection"]["auto_sensitivity_tuning"]:
                    current_stddev = float(results["stddev"])
                    
                    # 1. Update Noise Baseline (EMA)
                    if self.noise_baseline_stddev is None:
                        self.noise_baseline_stddev = current_stddev
                    else:
                        self.noise_baseline_stddev = ((1 - self.cfg["detection"].get("noise_smoothing_alpha")) * self.noise_baseline_stddev) + (self.cfg["detection"].get("noise_smoothing_alpha") * current_stddev)

                    # 2. Calculate the Target Threshold (Noise + Moon)
                    base_thr = self.cfg["detection"]["base_threshold"]
                    noise_factor = self.cfg["detection"]["noise_factor"]
                    # moon_penalty = (40 * moon_impact) if moon_in_fov else (15 * moon_impact)
                    moon_penalty = self.compute_moon_penalty(moon_impact=moon_impact, moon_alt=moon_alt, moon_in_fov=moon_in_fov)

                    # This is what the system *wants* the threshold to be
                    target_threshold = int(base_thr + (self.noise_baseline_stddev * noise_factor) + moon_penalty)

                    # 3. Apply Smoothing (Limit change to ±20% of current value)
                    if self.effective_threshold is None:
                        # First run initialization
                        self.effective_threshold = target_threshold
                    else:
                        # Smoothing: don't let it jump more than 20% in one step
                        max_delta = max(2, int(self.effective_threshold * 0.2))
                        
                        if target_threshold > self.effective_threshold:
                            self.effective_threshold = min(self.effective_threshold + max_delta, target_threshold)
                        elif target_threshold < self.effective_threshold:
                            self.effective_threshold = max(self.effective_threshold - max_delta, target_threshold)

                    # 4. Final Clamp (Absolute safety limits)
                    self.effective_threshold = max(self.threshold_min, min(self.threshold_max, self.effective_threshold))
                    
                    # 5. Update status for Dashboard/ZMQ
                    with self.status_lock:
                        # self.last_sky_conditions = results
                        self.last_moon_info = f"{int(moon_impact*100)}% (In FOV: {moon_in_fov})"
                        self.last_effective_threshold = self.effective_threshold

                    logging.info(
                        f"Sky Monitor: Noise StdDev={self.noise_baseline_stddev:.2f}, "
                        f"Moon Impact={moon_impact:.2f}, Effective Threshold={self.effective_threshold}")
                else:
                    # Static Mode
                    self.effective_threshold = self.cfg["detection"]["static_threshold"]
                    with self.status_lock:
                        self.last_effective_threshold = self.effective_threshold
                        
                    self.last_moon_info = f"{int(moon_impact*100)}% (In FOV: {moon_in_fov})"
                    
                meta = self.get_metadata(n_frames=1, override_exposure=snap_exposure_us, override_gain=snap_gain)
                meta.update(results)
                self.save_json_atomic(os.path.splitext(full_path)[0] + ".json", meta)
                logging.info("Saved daylight snapshot and analysis to %s", full_path)

                # 1. Get Base Config Limits
                cfg_day = self.cfg["daylight"]
                limit_std = cfg_day["stddev_threshold"]
                limit_stars = cfg_day["min_stars"]

                # 2. Apply Lunar Relaxation
                # If the moon is IN FOV, it creates massive contrast (high stddev).
                # We must relax the limit, otherwise the moon itself makes the system think it's "Cloudy".
                if moon_in_fov:
                    # If moon is visible, allow 4x the standard deviation
                    # and accept fewer stars (since the glare hides faint ones)
                    effective_std_limit = limit_std * 2.5
                    effective_star_limit = max(3, int(limit_stars * 0.4))
                    self.session_stats["moon_visible_minutes"] += elapsed_minutes
                    # TODO change the value of std_dev and star also in the dashboard
                    logging.info(f"Sky Check: Moon IN FOV. Relaxing limits -> MaxStd: {effective_std_limit:.1f}, MinStars: {effective_star_limit}")
                else:
                    # Normal Dark Sky
                    effective_std_limit = limit_std
                    effective_star_limit = limit_stars

                # 3. Perform the Check
                # We check results["stddev"] against the dynamic limit
                # is_clear = (float(results["stddev"]) <= effective_std_limit and results["stars"] >= effective_star_limit)
                # score = self.compute_sky_score(norm_stddev=results["norm_stddev"], stars=results["stars"], std_limit=effective_std_limit, star_limit=effective_star_limit)
                # score = self.compute_sky_score(stddev=results["stddev"], stars=results["stars"], std_limit=effective_std_limit, star_limit=effective_star_limit)
                score = self.compute_sky_score(
                    raw_stddev=results["stddev"],
                    norm_stddev=results["norm_stddev"],
                    mean_brightness=results["mean"],
                    stars=results["stars"], 
                    std_limit=effective_std_limit, 
                    star_limit=effective_star_limit
                )
                
                # | Score     | Stato              |
                # | --------- | ------------------ |
                # | ≥ 1.3     | Sereno ottimo      |
                # | 1.0 – 1.3 | Sereno accettabile |
                # | 0.8 – 1.0 | Borderline         |
                # | < 0.8     | Cloudy             |
                self.session_stats["avg_sky_score"].append(score)
                self.sky_score = score
                is_clear = score >= 0.8   # normally it was 1

                logging.info(f"SkyScore={score:.2f} | NormSTD={results['norm_stddev']:.4f} | Stars={results['stars']} | MoonPen={moon_penalty:.1f}")
                logging.info(f"moon_alt={moon_alt:.2f} | moon_in_fov={moon_in_fov}")
                
                # # --- Return Detailed Status Instead of a Simple Boolean ---
                # is_clear = (results["stddev"] <= self.cfg["daylight"]["stddev_threshold"] and results["stars"] >= self.cfg["daylight"]["min_stars"])

                if is_clear:
                    min_stars = self.cfg["daylight"]["min_stars"]
                    stddev_thr = self.cfg["daylight"]["stddev_threshold"]
                    
                    logging.info(f"Sky conditions: CLEAR (Stars={results['stars']} >= Min={min_stars}), STDDEV (stddev={results['stddev']:.2f} <= Max={stddev_thr:.2f})")
                    self.base_timelapse_interval = self.cfg["timelapse"]["nightmode_interval_sec"]
                    # self.timelapse_interval_sec = self.cfg["timelapse"]["nightmode_interval_sec"]
                    self.timelapse_interval_sec = self.base_timelapse_interval
                    self.session_stats["clear_sky_minutes"] += elapsed_minutes
                    return SkyConditionStatus.CLEAR, results
                else:
                    logging.warning("Sky conditions: CLOUDY/POOR (stddev=%.2f, stars=%d)", results["stddev"], results["stars"])
                    # If cloudy we don't need to acquire timelapse too fast
                    if self.timelapse_interval_sec <= cloudy_interval_time:      # 60sec
                        logging.info(f"The conditions are cloudy, so timelapse interval time is increase to {cloudy_interval_time} sec.")
                        self.base_timelapse_interval = cloudy_interval_time
                        # self.timelapse_interval_sec = cloudy_interval_time
                        self.timelapse_interval_sec = self.base_timelapse_interval
                    self.session_stats["cloudy_sky_minutes"] += elapsed_minutes
                    return SkyConditionStatus.CLOUDY, results
          
            except Exception as e:
                logging.exception("An error occurred during daylight snapshot: %s", e)
                # logging.exception("Diagnostics error: %s", e)
                # Ensure even on error, last_moon_status doesn't disappear from memory
                self.session_stats["error_mins"] += elapsed_minutes
                with self.status_lock:
                     # Informative dashboard message
                     self.last_moon_info = "SENSOR_ERROR"
                return SkyConditionStatus.ERROR, {"message": str(e)}
            
            finally:
                self.session_stats["last_check_time"] = now_ts
                self._set_camera_preset("NIGHT")
                logging.info("Restored night capture settings.")
    # -----------------
    def analyze_sky_conditions(self, img):
        """
        Analyzes the image to determine sky clarity.
        Improved to filter out sensor noise and cloud textures.
        """
        # 1. Get robust stats and the ROI image directly from the helper
        roi_mean, norm_stddev, raw_stddev, _, roi_img, _, _ = self._compute_roi_stats(img, blur=False)
         
        # --- STEP 2: Top-Hat Transform ---
        # Kernel size 15 is good for stars. 
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (15, 15))
        tophat = cv2.morphologyEx(roi_img, cv2.MORPH_TOPHAT, kernel)
        
        # --- STEP 3: Thresholding ---
        # th_mean, th_stddev = cv2.meanStdDev(tophat)
        th_mean, _ = cv2.meanStdDev(tophat)
        
        # # Adaptive sigma (cloudy skies need higher sigma)
        # sigma = 3.0
        # if th_stddev < 22.0:
            # sigma = 5.0

        # We ensure the threshold is at least 15 to avoid counting black-level noise.
        thresh_val = th_mean[0][0] + (3.0 * raw_stddev)
        
        # _, thresh_img = cv2.threshold(tophat, thresh_val, 255, cv2.THRESH_BINARY)
        _, thresh_img = cv2.threshold(tophat.astype(np.float32), thresh_val, 255, cv2.THRESH_BINARY)
        thresh_img = thresh_img.astype(np.uint8)

        # Remove isolated speckles after threshold
        clean_kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
        thresh_img = cv2.morphologyEx(thresh_img, cv2.MORPH_OPEN, clean_kernel)

        # --- STEP 4: Contour Analysis with Shape Filtering ---
        cnts, _ = cv2.findContours(thresh_img, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        star_count = 0
        for c in cnts:
            area = cv2.contourArea(c)
            
            # Filter 1: Area. 
            # Increase minimum to 3 to ignore sensor 'hot pixels' or tiny noise speckles.
            if 3 <= area <= 300:
                # Filter 2: Circularity (4 * pi * Area / Perimeter^2)
                # Stars are mostly circular (value near 1.0). Noise/Clouds are irregular.
                perimeter = cv2.arcLength(c, True)
                if perimeter == 0: continue
                circularity = 4 * np.pi * (area / (perimeter * perimeter))
                
                if circularity > 0.6: # Stars are usually > 0.7, 0.6 is safe.
                    star_count += 1

        # Debugging
        if self.cfg["general"].get("debug_visualization", False):
             logging.debug(f"[SkyCheck] Stars={star_count}, StdDev={raw_stddev:.2f}, NormStdDev={norm_stddev:.4f}")

        # return {"stddev": raw_stddev, "norm_stddev": norm_stddev, "mean":roi_mean, "stars": star_count}
        return {"stddev": raw_stddev / 256.0, "norm_stddev": norm_stddev, "mean": roi_mean / 256.0, "stars": star_count}
    # -----------------
    def compute_moon_penalty(self, moon_impact, moon_alt, moon_in_fov, moon_scale_in_fov=45, moon_scale_out_fov=18):
        """
        moon_impact: 0..1 (fase * illuminazione)
        moon_alt: gradi sopra l'orizzonte
        moon_in_fov: bool
        """
        if moon_alt <= 0:
            return 0.0

        # altezza normalizzata (0.2 minimo per evitare zero)
        alt_factor = max(0.2, min(moon_alt / 60.0, 1.0))

        # scala base
        scale = moon_scale_in_fov if moon_in_fov else moon_scale_out_fov

        # impatto NON lineare
        penalty = scale * (moon_impact ** 1.5) * alt_factor
        return penalty
    # -----------------
    def compute_sky_score(self, raw_stddev, norm_stddev, mean_brightness, stars, std_limit, star_limit):
        """
        Hybrid Scoring: Uses Raw limit for dark skies, and Normalized texture check for bright skies.
        """
        # --- FAILSAFE: Pure saturation check ---
        # If the image is completely blown out (pure white), it has 0 standard deviation, 
        # which tricks the math into thinking it's a perfectly clean dark sky.
        if mean_brightness > 245.0:
            return 0.0

        # 1. Star Score (Linear)
        star_score = stars / max(star_limit, 1)

        # 2. Noise Score (Hybrid)
        
        # A. The Strict Check (Raw)
        # If Raw Noise is low (e.g. 5.0 vs Limit 10.0), the sky is definitely clear.
        raw_score = std_limit / max(raw_stddev, 0.1)
        
        if raw_score >= 1.0:
            # Pass! The sky is dark and quiet.
            final_noise_score = raw_score
        else:
            # Fail! The sky is noisy (Raw > Limit). 
            # NOW we ask: Is it Clouds or just Brightness?
            
            # If the sky is pitch black (Mean < 5), Normalized math is unstable. 
            # We must accept the Raw failure.
            if mean_brightness < 5.0:
                final_noise_score = raw_score
            else:
                # The sky is bright. Check Texture (Normalized).
                # Empirical Rule: Clear sky NormStd is usually < 0.10. Clouds are > 0.15.
                # We set a "Texture Limit" of 0.12
                TEXTURE_LIMIT = 0.12
                
                norm_score = TEXTURE_LIMIT / max(norm_stddev, 0.001)
                
                # If norm_score > 1.0, it means the image is smooth (Glare/Light), not lumpy (Clouds).
                # We "Forgive" the high raw noise by taking the better score.
                final_noise_score = max(raw_score, norm_score)

        # Weights
        W_STD = 0.6
        W_STAR = 0.4

        score = (W_STD * final_noise_score) + (W_STAR * star_score)
        return score
    # -----------------
    def wait_for_safe_window(self):
        """
        Blocks until no event or timelapse stack is active.
        Returns False if shutdown is requested.
        """

        # 1. Wait for meteor event
        if self.event_in_progress.is_set():
            logging.warning("Sky Monitor: Event in progress. Deferring checks.")
            
            # Loop until event clears OR system stops
            while self.event_in_progress.is_set():
                if not self.running.is_set():
                    return False # Abort immediately on shutdown
                time.sleep(1.0)
                    
            logging.info("Sky Monitor: Event finished.")

        # 2. Check if Timelapse thread is actually running to avoid unnecessary waiting
        timelapse_alive = any(t.name == "TimelapseThread" and t.is_alive() for t in self.worker_threads)

        if not timelapse_alive:
            logging.info("Sky Monitor: Timelapse thread not active. Proceeding immediately.")
            return True

        logging.info("Sky Monitor: Waiting for current timelapse stack to finish...")

        self.timelapse_complete_event.clear()

        exp_sec = self.current_exposure_us / 1_000_000.0
        # We assume at least 1.5s per frame total cycle (Exposure + CPU Processing + Overhead)
        # If exposure is long (e.g. 4s), we use that + 0.5s overhead.
        cycle_time = max(1.5, exp_sec + 1.5)
        # Calculate timeout based on stack size
        # Timeout: (Frames * Cycle) + 30s Buffer
        wait_timeout = (self.cfg["timelapse"]["stack_N"] * cycle_time) + 30

        if not self.timelapse_complete_event.wait(timeout=wait_timeout):
            logging.warning(f"Sky Monitor: Timed out ({wait_timeout:.1f}s) waiting for timelapse. Proceeding anyway.")
        else:
            logging.info("Sky Monitor: Safe window detected.")

        return True
    # -----------------
    def _compute_roi_stats(self, frame, blur=False, star_candidates=None, radius=5, min_features=3, min_mask_pixels=50, save_debug=None, debug_out_dir=False):
        """
        Centralized ROI authority.

        - Computes ROI statistics
        - Defines and enforces central ROI
        - Optionally builds and validates a star mask

        Returns:
            mean, stddev, gray, roi, borders, roi_meta
        """
        
        # Convert to grayscale if needed
        if frame.ndim == 3:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        else:
            gray = frame
        
        # Apply blur if requested (useful for noise floor estimation, bad for star counting)
        if blur:
            gray = cv2.GaussianBlur(gray, (5,5), 0)
            
        h, w = gray.shape
        
        # Calculate offsets
        border_h = h // 4
        border_w = w // 4
        
        # ROI: Central 50% (cut top/bottom/left/right 25%)
#        roi = gray[h//4 : h*3//4, w//4 : w*3//4]
        roi = gray[border_h : h - border_h, border_w : w - border_w]
        mean, stddev = cv2.meanStdDev(roi)
        
        mean_val = float(mean[0][0])
        stddev_val = float(stddev[0][0])
        
        # Normalizza lo stddev per il livello medio
        norm_stddev = stddev_val / max(mean_val, 1.0)

        # roi_meta = {"stddev": float(stddev[0][0]), "stars_center": 0, "mask": None, "valid": False}
        roi_meta = {"stddev": stddev_val, "norm_stddev": norm_stddev, "stars_center": 0, "mask": None, "valid": False}

        # --- Optional mask construction ---
        if star_candidates:
            mask = np.zeros_like(gray, dtype=np.uint8)

            for cx, cy, _ in star_candidates:
                cv2.circle(mask, (int(round(cx)), int(round(cy))), radius, 255, -1)

            # Dilate slightly for ECC stability
            kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
            mask = cv2.dilate(mask, kernel, iterations=1)

            # Enforce ROI
            mask[:border_h, :] = 0
            mask[h - border_h :, :] = 0
            mask[:, :border_w] = 0
            mask[:, w - border_w :] = 0

            # Count stars inside ROI
            stars_in_center = sum(
                1 for cx, cy, _ in star_candidates
                if border_w < cx < w - border_w and border_h < cy < h - border_h
            )

            roi_meta.update({"stars_center": stars_in_center, "mask": mask, "valid": (cv2.countNonZero(mask) >= min_mask_pixels and stars_in_center >= min_features)})

        # --- Debug visualization ---
        if save_debug and debug_out_dir:
            try:
                debug_vis = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

                # ROI rectangle
                cv2.rectangle(
                    debug_vis,
                    (border_w, border_h),
                    (w - border_w, h - border_h),
                    (255, 0, 0),
                    2
                )

                # Stars
                for cx, cy, _ in star_candidates:
                    center = (int(cx), int(cy))
                    if border_w < cx < w - border_w and border_h < cy < h - border_h:
                        cv2.circle(debug_vis, center, 5, (0, 255, 0), 1)
                    else:
                        cv2.circle(debug_vis, center, 3, (0, 0, 255), 1)

                ts = datetime.now().strftime("%H%M%S")
                out_path = os.path.join(debug_out_dir, f"debug_stack_roi_{ts}.jpg")
                # cv2.imwrite(out_path, debug_vis)
                cv2.imwrite(out_path, (debug_vis >> 8).astype(np.uint8))

                logging.info(f"Saved stack ROI debug image to {out_path}")

            except Exception as e:
                logging.error(f"Failed to save ROI debug visualization: {e}")

        # return mean[0][0], stddev[0][0], gray, roi, (border_h, border_w), roi_meta
        return mean_val, norm_stddev, stddev_val, gray, roi, (border_h, border_w), roi_meta
    # -----------------
    def stack_frames(self, frames, align=True, method="mean", min_features_for_alignment=3, is_event_stack=False):
        """
        A method to align and stack a list of frames.
        It uses an iterative approach for 'mean' methods,
        method should be 'mean'.
        """
        if not frames:
            return None, "aborted", 0
        
        debug_level = self.cfg["general"].get("debug_level", 0)
        stack_dump_dir = None
        dump_root = None
        debug_mode = False
#        top_stars = 0
        brightness_boost = 1.2  # if astro_stretch is disabled this give a slightly bright boost
        
        # Setup disk dumping if Level = 2
        # if debug_level = 2 and self.debug_stack_counter < self.debug_stack_limit:
        if debug_level == 2:   
            timestamp = datetime.now().strftime("%H%M%S")
            stack_type = "event" if is_event_stack else "timelapse"
            # stack_dump_dir = os.path.join(self.general_log_dir, "debug_dumps", f"stack_{self.debug_stack_counter}_{stack_type}_{timestamp}")
            stack_dump_dir = os.path.join(self.general_log_dir, "debug_dumps", f"stack_{stack_type}_{timestamp}")     
            os.makedirs(stack_dump_dir, exist_ok=True)
            # self.debug_stack_counter += 1
        
        try:
            # Basic frame parameters
            h, w = frames[0].shape[:2]
            # feature_count = 0
            stars_used = 0
            # first_frame_gray = cv2.cvtColor(frames[0], cv2.COLOR_BGR2GRAY)
            half_stack = len(frames) // 2
            first_frame_gray = cv2.cvtColor(frames[half_stack], cv2.COLOR_BGR2GRAY)
                    
#            mean_brightness = np.mean(first_frame_gray)
            
            if debug_level >= 2:
#                dump_root  = os.path.join(self.general_log_dir, "debug_dumps")
                debug_mode = True
            else:
                debug_mode = False
            
            def create_star_mask(gray_img, percentile=97.0, min_area=2, max_area=400, radius=None, max_stars=200, min_features=3, save_debug=debug_mode, bias_mode=False, debug_out_dir=stack_dump_dir):
                try:
                    if radius is None:
                        radius = self.star_mask_radius
                        
                    # -------------------------------------------------
                    # 0. ROI statistics (central 50%)
                    # -------------------------------------------------
                    mean, _, stddev, gray_img, _, _, _ = self._compute_roi_stats(gray_img, blur=False)  # ok
                    # border_h, border_w = borders

                    # CAN REMOVE...?
                    # Early reject: no texture → no stars
                    if stddev < 0.2:   # 2 lower for cleaner/darker skies
                        #return None
                        return None, {"reason": "early_reject", "stddev": stddev}

                    # h, w = gray_img.shape

                    # -------------------------------------------------
                    # 1. Adaptive threshold (ROI-safe)
                    # -------------------------------------------------
                    # Primary: sigma-based (stable)
                    t_sigma = mean + (1.5 if bias_mode else 2.0) * stddev

                    # Fallback: percentile (for very sparse star fields)
                    t_percentile = np.percentile(gray_img, percentile)

                    # Final threshold
                    tval = max(3, t_sigma, t_percentile)

                    _, thresh = cv2.threshold(gray_img, tval, 255, cv2.THRESH_BINARY)
                    thresh = thresh.astype(np.uint8)

                    # -------------------------------------------------
                    # 2. Connected components
                    # -------------------------------------------------
                    nlabels, labels, stats, centroids = cv2.connectedComponentsWithStats(
                        thresh, connectivity=8
                    )

                    candidates = []

                    for i in range(1, nlabels):  # skip background
                        area = stats[i, cv2.CC_STAT_AREA]

                        # Filter hot pixels and large blobs
                        if min_area <= area <= max_area:
                            cx, cy = centroids[i]
                            ix, iy = int(cx), int(cy)

                            # if 0 <= iy < h and 0 <= ix < w:
                            if 0 <= iy < gray_img.shape[0] and 0 <= ix < gray_img.shape[1]:
                                # brightness = gray_img[iy, ix]
                                candidates.append((cx, cy, gray_img[iy, ix]))

                    if len(candidates) < min_features:
                        #return None
                        return None, {"reason": "early_reject", "stddev": stddev}

                    # -------------------------------------------------
                    # 3. Sort by brightness
                    # -------------------------------------------------
                    candidates.sort(key=lambda x: x[2], reverse=True)
                    max_stars = 400 if bias_mode else 200
                    top_stars = candidates[:max_stars]

                    # --- Delegate ROI + mask logic ---
                    _, _, _, _, _, _, roi_meta = self._compute_roi_stats(gray_img, blur=False, star_candidates=top_stars, radius=radius, min_features=min_features, save_debug=save_debug, debug_out_dir=debug_out_dir)    # ok

                    if not roi_meta["valid"]:
                        return None, {"reason": "early_reject", "stddev": stddev}

                    return roi_meta["mask"], {"candidates": len(candidates), "stars_center": roi_meta["stars_center"], "stddev": stddev}

                except Exception as e:
                    logging.error(f"Mask generation error: {e}")
                    return None, {"reason": "early_reject", "stddev": float(stddev)}

#----------------------

# ── Tuning philosophy
# This stretch is designed to adapt continuously from very low-light scenes
# (sensor noise dominates, signal sparse) to true astro data (clear signal above
# background). Key ideas:
#
# - Robust statistics (median, MAD, percentiles) are used instead of mean/variance
#   to avoid stars, hot pixels, and cosmic rays biasing estimates.
# - "low_lux_weight" represents how noise-dominated the image is, based on
#   absolute luminance level rather than user mode switches.
# - Multiple stretch models are blended rather than switched:
#     * perceptual gamma-like lift for extreme low light,
#     * hybrid gamma + arcsinh for transition regions,
#     * true arcsinh for astro-safe signal expansion.
# - Background subtraction, neutralization, and ratio amplification are
#   progressively reduced as lux drops to avoid destroying faint signal.
# - Color is preserved by reinjecting luminance ratios, with conservative
#   clamping in noisy regimes to prevent chroma blow-up.
#
# The numeric constants are empirically tuned to favor noise safety and
# star integrity over maximum contrast.

            def astro_stretch(img,bias_mode=False):
                neutralize_bg=False
                black_percentile=0.8
                white_percentile=99.7
                stretch_factor=3.0
                bg_subtract=True
                bg_sigma=120
                bg_scale=0.25
                protect_bg_percentile=2.0
                denoise_strength=0.25
                bg_subtract_lux_limit=0.85
                band_suppression_gain=0.15
                
                # TODO try to leave strength = 100 and change only stretch_amount
                # 0 -> original image
                # 100 -> full stretched
                # strength = self.cfg["timelapse"].get("astro_stretch_strength")
                strength = 100
                stretch_amount = 50
                strength = np.clip(strength, 0, 100) / 100.0
                stretch_amount = np.clip(stretch_amount, 0, 100) / 100
                strength = strength ** 1.6
                
                img = img.astype(np.float32)
                
                # Keep original for final safety blend
                img_orig = img.copy()

                # ── Normalize input
                maxv = img.max()
                if maxv > 1.5:
                    img /= maxv
                    img_orig /= maxv
                elif maxv > 1.0:
                    img /= 255.0
                    img_orig /= 255.0

                # ── Luminance (BT.709, OpenCV BGR-safe)
                lum = cv2.transform(img, np.array([[0.0722, 0.7152, 0.2126]], np.float32)).squeeze()

                # ── Robust signal / noise estimation (lux-aware)
                median_lum = np.median(lum)
                p90 = np.percentile(lum, 90)

                # ── Determine low-lux regime strength (smooth)
                # 0 → astro-safe, 1 → very low light
                low_lux_weight = np.clip((0.008 - median_lum) / 0.006, 0, 1)

                # ── Bias/preview override
                if bias_mode:
                    stretch_factor = 10.0
                    bg_scale *= 0.7
                    bg_sigma = 140
                    white_percentile = 99.9
                    denoise_strength = 0.0
                    low_lux_weight = min(low_lux_weight, 0.4)
                
                # Stage 1 — Pre-stretch noise shaping (VERY mild)
                if strength > 0 and low_lux_weight > 0.2:
                    img = cv2.fastNlMeansDenoisingColored((img * 255).astype(np.uint8), None, h=2.0 * strength, hColor=2.5 * strength, templateWindowSize=7, searchWindowSize=21).astype(np.float32) / 255.0

                # ── Black point subtraction (suppressed at low lux)
                if low_lux_weight < 0.7 and strength > 0:
                    black = np.percentile(lum, black_percentile)
                    black *= (1 - low_lux_weight) * strength
                    img = np.clip(img - black, 0, None)
                    lum = np.clip(lum - black, 0, None)

                # ── Optional background neutralization (astro regime only)
                if neutralize_bg and low_lux_weight < 0.5 and strength > 0:
                    mask = lum < np.percentile(lum, 5.0)
                    if np.any(mask):
                        bg_bgr = np.median(img[mask], axis=0)
                        gray = np.mean(bg_bgr)
                        if gray > 1e-6:
                            scale = gray / np.maximum(bg_bgr, 1e-6)
                            scale = np.clip(scale, 0.75, 1.35)
                            img *= 1 + strength * (scale - 1)

                # ── White point normalization (astro regime weighted)
                white = np.percentile(lum, white_percentile)
                if white > 0:
                    img /= max(white, 1e-6)
                    lum /= max(white, 1e-6)
                    
                img = np.clip(img, 0, 1)
                lum = np.clip(lum, 0, 1)

                if low_lux_weight > 0.6 and band_suppression_gain > 0 and strength > 0:
                    lum -= cv2.GaussianBlur(lum, (0, 0), sigmaX=8, sigmaY=40) * band_suppression_gain * strength
                    lum = np.clip(lum, 0, 1)

                # ── Gradient removal (disabled gradually at low lux)
                if bg_subtract and bg_sigma > 0 and low_lux_weight < bg_subtract_lux_limit and strength > 0:
                    bg = cv2.GaussianBlur(lum, (0, 0), sigmaX=bg_sigma)
                    protect_th = np.percentile(lum, protect_bg_percentile)
                    protect_mask = (lum < protect_th).astype(np.float32)
                    protect_mask = cv2.GaussianBlur(protect_mask, (0, 0), sigmaX=bg_sigma / 5)
                    effective_bg_scale = bg_scale * (1 - 0.6 * low_lux_weight)
                    effective_bg_scale *= strength
                    #effective_scale = effective_bg_scale * (1 - protect_mask)
                    # img -= effective_scale[..., None] * bg[..., None]
                    img -= effective_bg_scale * (1 - protect_mask)[..., None] * bg[..., None]
                    img = np.clip(img, 0, None)

                # ── Recompute luminance
                lum = cv2.transform(img, np.array([[0.0722, 0.7152, 0.2126]], np.float32)).squeeze()
                lum = np.clip(lum, 0, 1)

                # ── Noise floor protection (clamped at low lux)
                noise_floor = min(np.percentile(lum, 2.0), 0.02)
                lum_safe = lum + noise_floor * (1 - lum)

                # Stage 2 — Luminance-only denoise (MOST IMPORTANT)
                if strength > 0 and low_lux_weight < 0.7:
                    lum_safe = cv2.GaussianBlur(lum_safe, (0, 0), sigmaX=0.6 + 0.8 * strength)  # end stage 2

                # ── Three stretch models
                # Low-lux perceptual
                lum_low = np.clip((lum_safe ** 0.45) * 2.0, 0, 1)

                # Hybrid
                lum_hybrid = (0.6 * (lum_safe ** 0.6) + 0.4 * (np.arcsinh(lum_safe * 3.0) / np.arcsinh(3.0)))

                # Astro arcsinh
                lum_astro = np.arcsinh(lum_safe * stretch_factor) / np.arcsinh(stretch_factor)

                # ── Blend stretches
                median_lum = np.median(lum)
                w_low = low_lux_weight
                w_astro = np.clip((median_lum - 0.012) / 0.01, 0, 1)
                w_hybrid = np.clip(1.0 - w_low - w_astro, 0, 1)

                lum_stretched = (w_low * lum_low + w_hybrid * lum_hybrid + w_astro * lum_astro)

                # ── Strength-scaled stretch
                lum_out = lum_safe + strength * (lum_stretched - lum_safe)

                # ── Color-preserving reinjection (ratio-safe)
                # ratio = lum_out / max(lum_safe, 1e-6)
                ratio = lum_out / np.maximum(lum_safe, 1e-6)
                ratio = np.clip(ratio, 0, 3.0 if low_lux_weight > 0.4 else 8.0)
                img *= ratio[..., None]

                # Stage 3 — Post-stretch background-only denoise
                if strength > 0 and low_lux_weight > 0.1:
                    bg_mask = lum_out < np.percentile(lum_out, 20)
                    bg_mask = cv2.GaussianBlur(bg_mask.astype(np.float32), (0, 0), sigmaX=3)
                    denoised = cv2.fastNlMeansDenoisingColored((img * 255).astype(np.uint8), None, h=3.0 * strength, hColor=2.5 * strength, templateWindowSize=7, searchWindowSize=21).astype(np.float32) / 255.0
                    img = img * (1 - bg_mask[..., None]) + denoised * bg_mask[..., None]  # end stage 3 

                # ── Final safety
                img = np.clip(img, 0, 1)
                
                # ── Final safety blend
                # img = img_orig + strength * (img - img_orig)
                img = img_orig + stretch_amount * (img - img_orig)
                return np.clip(img, 0.0, 1.0).astype(np.float32)
                
            effective_method = method

            # ----------------------------------------------------------------------
            # 4. Pre-Alignment Checks
            # ----------------------------------------------------------------------
            perform_alignment = align
            star_mask = None
            bias_mode = False
            bias_reasons = []
            stars_used = 0

            if perform_alignment: 
                
                # Attempt star-mask creation
                # --- STAR MASK CREATION ---
                if self.last_sky_status != SkyConditionStatus.CLOUDY:
                    star_mask, star_meta = create_star_mask(first_frame_gray, min_features=min_features_for_alignment, save_debug=debug_mode, bias_mode=False, debug_out_dir=stack_dump_dir)
                else: 
                    logging.info("Alignment skipped: SkyMonitor reports CLOUDY.")
                    star_mask = None
                    star_meta = {"stddev": 0}
                
                if star_mask is None:
                    bias_mode = True
                    perform_alignment = False
                    bias_reasons.append("no_star_mask")
                else:
                    stars_used = star_meta["stars_center"]

                    if stars_used < min_features_for_alignment:
                        bias_mode = True
                        perform_alignment = False
                        bias_reasons.append(f"low_star_count:{stars_used}")

                    elif star_meta.get("stddev", 0) < self.alignment_contrast_floor:    # 2.0 lower if is too strict and with a perfect sky, pass to bias mode
                        bias_mode = True
                        perform_alignment = False
                        bias_reasons.append(f"low_contrast:{star_meta.get('stddev',0):.2f}")

                    else:
                        bias_mode = False
                        perform_alignment = True

                # bias_mode = False

                # --- Notification for ZMQ Viewer ---
                if self.cfg["general"].get("debug_level") == 3:
                    if bias_mode:
                        # Option A: Send a JSON status
                        self.publish_debug("stack_status", {
                            "mode": "BIAS_MODE",
                            "action": "ALIGMENT_SKIPPED",
                            "reason": bias_reasons,
                            "frames": len(frames)
                        })
                        
                        # Option B: Send a visual placeholder (Black image with text)
                        # This ensures the viewer screen updates so you know it's not "frozen"
                        placeholder = np.zeros((400, 600, 3), dtype=np.uint8)
                        cv2.putText(placeholder, "BIAS MODE: NO ALIGNMENT", (50, 180), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 165, 255), 2)
                        cv2.putText(placeholder, f"Reason: {', '.join(bias_reasons)}", (50, 230), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1)
                        
                        self.publish_debug("alignment_mosaic", {"status": "skipped"}, image=placeholder)
                    else:
                        self.publish_debug("stack_status", {
                            "mode": "NORMAL",
                            "action": "ALIGNMENT_STARTING",
                            "frames": len(frames)
                        })

                # If we have NO mask and are in bias mode, we can't align via ECC anyway.
                # So we disable alignment but keep the "Bias" stretch parameters.
                if star_mask is None:
                    logging.info("Alignment disabled: insufficient usable stars even for bias mode.")
                    perform_alignment = False

#                if star_mask is None:
#                    star_meta = {"stddev": 0}
                logging.info(f"Bias mode: {'ON' if bias_mode else 'OFF'} (stars={stars_used}, std={star_meta.get('stddev',0):.2f})")

                if star_mask is None or cv2.countNonZero(star_mask) < 100:
                    perform_alignment = False

                self.publish_debug(
                    "stack_bias",{
                        "enabled": bias_mode,
                        "reasons": bias_reasons,
                        "stars": stars_used,
                        "stddev": star_meta.get("stddev", 0),
                        "frames": len(frames),
                    }
                )

                if bias_mode:
                    logging.warning("STACK BIAS MODE ENABLED | reasons=%s | stars=%d | stddev=%.2f | frames=%d", ",".join(bias_reasons), stars_used, star_meta.get("stddev", 0), len(frames))
                else:
                    logging.info("STACK NORMAL MODE | stars=%d | stddev=%.2f | frames=%d", stars_used, star_meta.get("stddev", 0), len(frames))

                # if not is_event_stack and star_mask is None:
                    # logging.warning("Timelapse alignment impossible → single-frame fallback.")
                    # return frames[len(frames)//2], "single_frame_fallback", 0
                
                if bias_mode:
                    used_method = "bias mode"
                else:
                    used_method = effective_method
                
                # ----------------------------------------------------------------------
                # 5. Alignment Loop
                # ----------------------------------------------------------------------
                cc_values = []
                weight_values = []
                consecutive_low_cc = 0          # NEW: drift-detection counter
                CC_DRIFT_THRESHOLD  = 0.15      # NEW: cc below this is "poor" for drift purposes
                CC_DRIFT_MAX_COUNT  = 3         # NEW: re-anchor after this many consecutive poor frames
                # keep a separate prepared ref so we can swap it without touching first_frame_gray
                current_ref_gray = first_frame_gray.copy()    # NEW
                # --- Processing Loop ---
                if effective_method == "mean":
                    accumulator = np.zeros((h, w, 3), dtype=np.float64)
                    weight_map  = np.zeros((h, w),    dtype=np.float64)  # per-pixel weight sum
                    actual_weight = 0.0
                    actual_frames_stacked = 0 
                    
                    ref_weight = 0.8 if bias_mode else 1.0
                    
                    # Add reference frame — all pixels valid, no warp applied
                    accumulator += frames[half_stack].astype(np.float64) * ref_weight
                    weight_map  += ref_weight
                    actual_weight += ref_weight
                    actual_frames_stacked += 1

                    for i in range(len(frames)):
                        if i == half_stack: continue # Skip reference
                        
                        frame_to_add = frames[i]
                        should_add = True 
                        weight = 0.9 if bias_mode else 1.0
                        alignment_meta = {"frame": i, "success": False, "cc": 0.0, "warp_norm": 0.0}

                        if perform_alignment:
                            try:
                                gray = cv2.cvtColor(frame_to_add, cv2.COLOR_BGR2GRAY)
                                
                                # ECC Config
                                # warp_mode = cv2.MOTION_EUCLIDEAN
                                warp_mode = cv2.MOTION_TRANSLATION if bias_mode else cv2.MOTION_EUCLIDEAN
                                # criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 100, 1e-6)
                                criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 200, 1e-4)
                                warp_matrix = np.eye(2, 3, dtype=np.float32)
                                
                                # Use a Top-Hat transform or a Sharpen filter to isolate stars:
                                k = max(3, self.star_mask_radius * 2 - 1)   # odd number matching star radius
                                if k % 2 == 0: k += 1
                                kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (k, k))
                                # kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
                                ref_prep = cv2.morphologyEx(current_ref_gray, cv2.MORPH_TOPHAT, kernel)
                                cur_prep = cv2.morphologyEx(gray, cv2.MORPH_TOPHAT, kernel)                                
                                
                                # ref_ecc = cv2.normalize(ref_prep, None, 0, 255, cv2.NORM_MINMAX)
                                # cur_ecc = cv2.normalize(cur_prep, None, 0, 255, cv2.NORM_MINMAX)
                                ref_ecc = cv2.normalize(ref_prep, None, 0, 255, cv2.NORM_MINMAX).astype(np.float32)
                                cur_ecc = cv2.normalize(cur_prep, None, 0, 255, cv2.NORM_MINMAX).astype(np.float32)
                                gauss_size = 5 if bias_mode else 3
                                # CAPTURE METRICS
                                cc, warp_matrix = cv2.findTransformECC(ref_ecc, cur_ecc, warp_matrix, warp_mode, criteria, inputMask=star_mask, gaussFiltSize=gauss_size)
                                
                                # Calculate Warp Magnitude (how much did we shift/rotate?)
                                # Norm of (Warp - Identity)
                                warp_diff = warp_matrix - np.eye(2, 3, dtype=np.float32)
                                warp_norm = float(np.linalg.norm(warp_diff))
                                MAX_WARP_PIXELS = min(w, h) * 0.10   # reject if shift > 10% of frame dimension
                                if warp_norm > MAX_WARP_PIXELS:
                                    logging.warning(f"Frame {i}: warp too large ({warp_norm:.1f}px), rejecting.")
                                    should_add = False
                                    alignment_meta["error"] = "excessive_warp"
                                
                                alignment_meta.update({"success": True, "cc": float(cc), "warp_norm": warp_norm})

                                cc_ref = 0.05 if bias_mode else 0.10
                                weight_floor = 0.0 if bias_mode else 0.3
                                
                                if cc < cc_ref:
                                    weight = max(weight_floor, cc / cc_ref)
                                else:
                                    weight = 1.0

                                if should_add:
                                    frame_to_add = cv2.warpAffine(frame_to_add, warp_matrix, (w, h), flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP)

                                cc_values.append(cc)
                                weight_values.append(weight)

                                # ── NEW: drift detection ────────────────────────────────────
                                if cc < CC_DRIFT_THRESHOLD  or not should_add:
                                    consecutive_low_cc += 1
                                else:
                                    consecutive_low_cc = 0
                                    # Good frame: promote it as the new reference so
                                    # future frames align to the most recent stable sky.
                                    current_ref_gray = gray.copy()

                                if consecutive_low_cc >= CC_DRIFT_MAX_COUNT:
                                    # Several frames in a row have drifted badly.
                                    # Force re-anchor to this frame regardless of its cc,
                                    # because the old reference is clearly no longer valid.
                                    logging.warning(
                                        "Frame %d: %d consecutive low-cc frames detected. "
                                        "Re-anchoring alignment reference.", i, consecutive_low_cc
                                    )
                                    current_ref_gray = gray.copy()
                                    consecutive_low_cc = 0
                                # ── end drift detection ──
                                     
                                if debug_level == 3:
                                    aligned_gray = cv2.cvtColor(frame_to_add, cv2.COLOR_BGR2GRAY)
                                    diff = cv2.absdiff(current_ref_gray, aligned_gray)
                                    mosaic = self.create_alignment_mosaic(current_ref_gray, gray, aligned_gray, diff)
                                    
                                    if mosaic is not None:
                                        # Level 2+: Save to disk if directory is set
                                        if stack_dump_dir:
                                            cv2.imwrite(os.path.join(stack_dump_dir, f"align_{i:03d}_cc{int(cc*100)}.jpg"), mosaic)
                                        
                                        # Level 3: Stream over ZMQ
                                        if debug_level >= 3:
                                            self.publish_debug("alignment_mosaic", {"frame_index": i, "correlation": float(cc)}, image=mosaic)

                            except cv2.error:
                                logging.warning(f"Frame {i}: ECC Alignment crashed.")
                                should_add = False
                                alignment_meta["error"] = "cv2_error"
                                weight = 0.8 if bias_mode else 0.0

                        # Publish stats via ZMQ
                        self.publish_debug("alignment", alignment_meta)

                        if should_add:
                            weight = np.clip(weight, 0.0, 1.0)
                            # accumulator += frame_to_add.astype(np.float64) * weight
                            
                            # Build per-pixel valid mask: pixels warped off-frame become 0.
                            # Using INTER_NEAREST so the mask is a clean binary 0/1 with
                            # no interpolation fringe at the border.
                            ones = np.ones((h, w), dtype=np.float32)
                            if perform_alignment:
                                valid_mask = cv2.warpAffine(ones, warp_matrix, (w, h), flags=cv2.INTER_NEAREST + cv2.WARP_INVERSE_MAP).astype(np.float64)
                            else:
                                valid_mask = ones.astype(np.float64)

                            accumulator += frame_to_add.astype(np.float64) * weight * valid_mask[..., None]
                            weight_map  += weight * valid_mask

                            actual_weight += weight
                            actual_frames_stacked += 1
                    
                    # ... [Keep Final Calculation Logic (Mean)] ...
                    if effective_method == "mean":
                        if actual_frames_stacked == 0: 
                            return None, "aborted", 0
                        if actual_weight <= 0:
                            return None, "aborted", 0
                        # stacked_float = accumulator / actual_weight
                        # Per-pixel division: each pixel is divided only by the sum of
                        # weights of frames that actually covered it (valid_mask > 0).
                        # Pixels where no frame landed remain black (weight_map == 0).
                        safe_weight = np.where(weight_map > 0, weight_map, 1.0)
                        stacked_float = accumulator / safe_weight[..., None]
                        stacked_float = np.where(weight_map[..., None] > 0, stacked_float, 0.0)
                    
                    logging.info(f"STACKED FRAMES: {actual_frames_stacked}/{len(frames)}") 

                    # 1. Normalize to 0.0 - 1.0 range
                    # final_float = stacked_float / 255.0
                    # Detect bit depth from the actual frame values:
                    max_val = 65535.0 if frames[0].dtype == np.uint16 else 255.0
                    final_float = stacked_float / max_val
                    final_float = np.clip(final_float, 0.0, 1.0).astype(np.float32)

                    if self.cfg["timelapse"].get("astro_stretch"):
                        #final_image = astro_stretch(stacked_float)
                        final_float = astro_stretch(final_float, bias_mode=bias_mode)
                    else:
                        # final_image = stacked_float.clip(0, 255).astype(np.uint32)
                        #final_image = np.clip(stacked_float * brightness_boost, 0, 255)
                        final_float = np.clip(final_float * brightness_boost, 0.0, 1.0)
                        
                    if cc_values:
                        logging.info("STACK ALIGN STATS | bias=%s | cc_med=%.3f | cc_min=%.3f | weight_mean=%.2f", bias_mode, float(np.median(cc_values)), float(np.min(cc_values)), float(np.mean(weight_values)))

                    info = {
                        "stars": stars_used,
                        "stddev": star_meta.get("stddev", 0),
                        "bias_reasons": bias_reasons if bias_mode else []
                    }                    
                    # 4. Convert to UINT16 (0 - 65535) for High Quality Saving
                    final_uint16 = (final_float * 65535).astype(np.uint16)
                    #return final_image, used_method, info
                    return final_uint16, used_method, info

                else:
                    return None, "aborted", 0

        except Exception as e:
            logging.error(f"Stacking Crash: {e}", exc_info=True)
            return None, "error", 0
    # -----------------
    def create_alignment_mosaic(self, ref, inp, aligned, diff):
        """Creates a 4-panel diagnostic image for alignment debugging."""
        try:
            h, w = ref.shape[:2]
            scale = 0.5
            
            # Helper to resize and convert to color if needed
            def prep(img, label):
                if img.dtype == np.uint16:
                    img = (img >> 8).astype(np.uint8)
                if len(img.shape) == 2:
                    img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
                res = cv2.resize(img, (int(w*scale), int(h*scale)))
                cv2.putText(res, label, (10, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
                return res

            # Normalize diff for visualization (amplify differences)
            diff_8 = (diff >> 8).astype(np.uint8)
            # diff_vis = cv2.normalize(diff, None, 0, 255, cv2.NORM_MINMAX)
            diff_vis = cv2.normalize(diff_8, None, 0, 255, cv2.NORM_MINMAX)
            diff_vis = cv2.applyColorMap(diff_vis, cv2.COLORMAP_JET)

            top = np.hstack((prep(ref, "Reference"), prep(inp, "Input")))
            bot = np.hstack((prep(aligned, "Aligned"), prep(diff_vis, "Diff Score")))
            return np.vstack((top, bot))
            # return (np.vstack((top, bot)) >> 8).astype(np.uint8)
        except Exception:
            return None
    # -----------------
    def get_moon_state(self):
        """Calculates Moon position, phase, and FOV status."""
        try:
            import ephem
            obs = ephem.Observer()
            obs.lat, obs.lon = str(self.cfg["general"]["latitude"]), str(self.cfg["general"]["longitude"])
            obs.date = datetime.now(timezone.utc)
            
            moon = ephem.Moon(obs)
            # Get raw radians first for math
            m_alt_rad, m_az_rad = float(moon.alt), float(moon.az)
            # Convert to degrees for display
            m_alt_deg, m_az_deg = np.degrees(m_alt_rad), np.degrees(m_az_rad)
            m_phase = moon.moon_phase 

            if m_alt_deg <= -5: return 0.0, False, m_alt_deg, 180.0

            # Camera Orientation
            c_alt = self.cfg["general"].get("camera_altitude", 90.0) 
            c_az = self.cfg["general"].get("camera_azimuth", 0.0)
            h_fov = self.cfg["general"].get("lens_hfov", 80.0)
            v_fov = self.cfg["general"].get("lens_vfov", 45.0)
            
            # Spherical Distance from Camera Center
            phi1, lam1 = np.radians(c_alt), np.radians(c_az)
            phi2, lam2 = m_alt_rad, m_az_rad
            
            # Clamping dot product to -1..1 to prevent math domain error on rounding
            dot_prod = np.sin(phi1)*np.sin(phi2) + np.cos(phi1)*np.cos(phi2)*np.cos(lam1-lam2)
            dot_prod = max(-1.0, min(1.0, dot_prod))
            
            dist = np.degrees(np.arccos(dot_prod))

            # If camera is at Zenith (90), Azimuth doesn't matter for distance.
            # We check if the distance is within the "Cone" of the lens.
            # Lens Radius approx = Average FOV / 2.
            # Using 1.0 divisor to use FULL glass coverage.
            lens_radius = (h_fov + v_fov) / 4.0 # Approx 42 degrees radius for your lens
            
            # Widen the radius slightly (x1.4) because the corners of a rectangular image reach further than the average radius
            safe_radius = lens_radius * 1.4
            
            in_fov = (dist < safe_radius)

            # Impact Scaling
            impact = (m_phase * (max(0, m_alt_deg)/90.0)) ** 1.2
            if in_fov: impact = min(1.0, impact + 0.4)

            return round(impact, 3), in_fov, round(m_alt_deg, 1), round(dist, 1)
            
        except Exception as e:
            logging.error(f"Moon Logic Error: {e}")
            return 0.0, False, 0.0, 180.0
    # -----------------
    def get_cosmic_narrative(self, score, stars, moon_up):
        if score < 0.8:
            return "The sky is veiled in clouds; the stars are resting."
        if moon_up and score > 1.2:
            return "The Moon dominates the heights, but the air is crystal clear."
        if stars > 40:
            return "A magnificent night; the deep sky is fully revealed."
        if score >= 1.0:
            return "The atmosphere is steady. We are watching."
        return "The sky is quiet."
    # -----------------
    # 6. OUTPUTS: EVENTS & TIMELAPSE
    # -----------------
    def event_writer_loop(self):
        logging.info("Event writer started (Direct-to-FFmpeg Pipe Mode).")
        out_dir = self.events_out_dir

        while True:
            try:
                # Wait for a complete package of frames
                frames = self.event_q.get()
                if frames is None:
                    logging.info("Event writer received sentinel. Exiting.")
                    break
            except queue.Empty:
                continue
            
            if not frames:
                logging.info("Received an empty event package, ignoring.")
                continue

            # Generate filename based on the last frame's timestamp
            tstamp = datetime.fromisoformat(frames[-1][0]).strftime("%Y%m%dT%H%M%SZ")
            base_name = f"event_{tstamp}"
            video_path = os.path.join(out_dir, base_name + ".mp4")

            # Determine video properties from the first frame
            first_frame = frames[0][1]
            height, width, layers = first_frame.shape
            
            fps = self.cfg["events"]["video_fps"]
            encoder = self.cfg["timelapse_video"]["ffmpeg_encoder"]
            # Ensure bitrate is a string (e.g., "2000k")
            bitrate = str(self.cfg["timelapse_video"]["ffmpeg_bitrate"])

            logging.info("Encoding event video to %s (%d frames, %dx%d)", base_name, len(frames), width, height)

            # Build FFmpeg command for reading raw video from stdin (pipe)
            ff_cmd = [
                "ffmpeg",
                "-y",                   # Overwrite output
                "-f", "rawvideo",       # Input format is raw
                "-vcodec", "rawvideo",
                "-s", f"{width}x{height}", # Resolution must be specified for raw input
                "-pix_fmt", "bgr24",    # OpenCV uses BGR
                "-r", str(fps),         # Input framerate
                "-i", "-",              # Read from stdin
                "-c:v", encoder,        # Output encoder (libx264 or h264_v4l2m2m)
                "-b:v", bitrate,        # Target bitrate
                "-pix_fmt", "yuv420p",  # Output pixel format (compatible with players)
                video_path
            ]

            process = None
            try:
                # Start FFmpeg process with a pipe for stdin
                process = subprocess.Popen(
                    ff_cmd, 
                    stdin=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    stdout=subprocess.DEVNULL
                )

                # Write frames directly to the pipe
                for _, frame, _ in frames:
                    try:
                        # Write raw bytes (fastest method, no JPEG encoding)
                        # process.stdin.write(frame.tobytes())
                        frame_u8 = (frame >> 8).astype(np.uint8)
                        process.stdin.write(frame_u8.tobytes())
                        # max_val = 65535.0 if frames[0].dtype == np.uint16 else 255.0
                        # final_float = stacked_float / max_val
                    except BrokenPipeError:
                        logging.error("FFmpeg pipe broke unexpectedly.")
                        process.kill()   # Ensure ffmpeg doesn't write a partial file silently.
                        break

                # Close stdin to signal EOF to FFmpeg
                stdout_data, stderr_data = process.communicate()

                if process.returncode != 0:
                    logging.error("FFmpeg Error:\n%s", stderr_data.decode("utf-8"))
                    self.log_health_event("ERROR", "FFMPEG_FAIL", "Non-zero return code")
                else:
                    self._tally_data_write(video_path)
                    self.save_event_metadata(video_path, frames)
                    logging.info("Event video saved: %s", video_path)

                    # Update dashboard status
                    with self.status_lock:
                        self.last_event_files["video"] = video_path

                    # Queue for SFTP
                    if self.sftp_uploader and self.running.is_set():
                        self.sftp_dispatch_q.put(video_path)
                        self.sftp_dispatch_q.put(os.path.splitext(video_path)[0] + ".json")

            except Exception as e:
                logging.exception("An error occurred in event_writer_loop: %s", e)
                # Ensure process is killed if python crashes
                if process:
                    try: process.kill()
                    except OSError:
                        pass
    # -----------------
    def event_stacker_loop(self):
        logging.info("Event Stacker thread started and waiting for packages.")

        out_dir = self.events_out_dir

        while self.running.is_set():
            try:
                
                cfg = self.cfg["timelapse"]
                method = cfg.get("stack_method", "mean")
                align = cfg.get("stack_align", True)
                alignment_features = cfg.get("min_features_for_alignment", 20)                
                
                event_frames = self.event_stack_q.get()
                if event_frames is None:
                    logging.info("Event Stacker received sentinel. Exiting.")
                    break
                if not event_frames:
                    continue

                logging.info("Event Stacker received package of %d frames.", len(event_frames))
                frames_to_stack = [f for (_, f, _) in event_frames]
                
                stacked_image, final_method, stack_info = self.stack_frames(
                    frames_to_stack, 
                    align=self.cfg["timelapse"]["stack_align"], 
                    method=self.cfg["timelapse"]["stack_method"], 
                    min_features_for_alignment=self.cfg["timelapse"]["min_features_for_alignment"],
                    is_event_stack=True 
                )

                if stacked_image is not None:
                    tstamp = datetime.fromisoformat(event_frames[-1][0]).strftime("%Y%m%dT%H%M%SZ")
                    out_name = f"event_stack_{tstamp}.png"
                    full_path = os.path.join(out_dir, out_name)
                    cv2.imwrite(full_path, stacked_image, [int(cv2.IMWRITE_PNG_COMPRESSION), 3])
                    self.save_event_stack_metadata(full_path, event_frames, final_method, stack_info)
                    logging.info("Saved event stack image to %s", full_path)
                    
                    # Update the dashboard state with the path to the new stacked image
                    with self.status_lock:
                        self.last_event_files["image"] = full_path

            except Exception as e:
                logging.exception("Error in event_stacker_loop: %s", e)
    # -----------------
    def timelapse_loop(self):
        first_run=True
        buffer = []

        while self.running.is_set():
            try:
                cfg = self.cfg["timelapse"]
                # interval_sec = self.timelapse_interval_sec
                if self.cfg["general"].get("debug_visualization", False):
                    logging.info(f"Timelapse started at {time.time()}")
                
                if first_run:
                    logging.info("Timelapse worker started (stack_N=%d, method=%s, align=%s)", cfg["stack_N"],cfg["stack_method"],cfg["stack_align"])
                    first_run=False
                
                item = self.timelapse_q.get()

                if item is None:
                    logging.info("Timelapse loop received sentinel.")
                    if buffer:
                        logging.info("Processing final timelapse stack before exiting...")
                        frames_to_stack = [f for (_, f, _) in buffer]
                        stacked_image, final_method, stack_info = self.stack_frames(
                            frames_to_stack, 
                            align=cfg["stack_align"], 
                            method=cfg["stack_method"], 
                            min_features_for_alignment=cfg["min_features_for_alignment"],
                            is_event_stack=False 
                        )
                        if stacked_image is not None and final_method is not None:
                            tstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                            out_name = f"timelapse_final_{tstamp}.png"
                            full_path = os.path.join(self.timelapse_out_dir, out_name)
                            cv2.imwrite(full_path, stacked_image, [int(cv2.IMWRITE_PNG_COMPRESSION), 3])
                            self._tally_data_write(full_path)
                            self.save_timelapse_metadata(full_path, final_method, buffer, stack_info)
                    logging.info("Draining complete, exiting.")
                    break # Exit the loop

                buffer.append(item)
            except queue.Empty:
                continue

            if len(buffer) >= cfg["stack_N"]:
                frames_to_stack = [f for (_, f, _) in buffer]
                stacked_image, final_method, stack_info = self.stack_frames(
                    frames_to_stack, 
                    align=cfg["stack_align"], 
                    method=cfg["stack_method"], 
                    min_features_for_alignment=cfg["min_features_for_alignment"],
                    is_event_stack=False
                )

                if stacked_image is not None and final_method is not None:
                    tstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    out_name = f"timelapse_{tstamp}.png"
                    full_path = os.path.join(self.timelapse_out_dir, out_name)
                    cv2.imwrite(full_path, stacked_image, [int(cv2.IMWRITE_PNG_COMPRESSION), 3])
                    logging.info("Saved timelapse stack to %s", full_path)
                    self.save_timelapse_metadata(full_path, final_method, buffer, stack_info)

                    if self.sftp_uploader and self.running.is_set():
                        logging.info(f"Queueing {os.path.basename(full_path)} and its metadata for upload.")
                        self.sftp_dispatch_q.put(full_path)
                        self.sftp_dispatch_q.put(os.path.splitext(full_path)[0] + ".json")
                        
                buffer.clear()

                logging.debug("Timelapse: Stack complete. Signaling safe window.")
                self.timelapse_complete_event.set()

                # if interval_sec > 0:
                    # # This logic for discarding is fine
                    # self.timelapse_next_capture_time = time.time() + interval_sec
                    
                    # while time.time() < self.timelapse_next_capture_time and self.running.is_set():
                        # try:
                            # self.timelapse_q.get(timeout=0.5)
                        # except queue.Empty:
                            # # time.sleep(0.1)
                            # pass

                if self.cfg["general"].get("debug_visualization", False):
                    logging.info(f"Timelapse finished at {time.time()}")
    # -----------------
    def start_timelapse_video_creation(self):
        """
        Launches the timelapse video creation process in a new, non-blocking
        daemon thread.
        """
        if not self.cfg["timelapse_video"].get("enabled", False):
            return

        logging.info("Scheduler has triggered end-of-session timelapse video creation.")
        # Run the potentially long video encoding process in a separate thread
        # so it doesn't block the scheduler or the main shutdown sequence.
        video_thread = threading.Thread(
            target=self._create_timelapse_video,
            name="TimelapseVideoThread",
            daemon=True # Daemon threads will not block program exit
        )
        video_thread.start()
    # -----------------
    def _create_timelapse_video(self):
        """
        Gathers all timelapse images from the last session and encodes them
        into a single MP4 video file using FFmpeg.
        """
        logging.info("Timelapse video creation thread started.")
        # Give other threads a moment to finish writing their last files
        time.sleep(10)

        if not self.cfg["timelapse_video"].get("enabled"):
            logging.info("Video creation is disabled")
            return

        session_start_ts = self.session_start_time.timestamp()

        # 1. Find all timelapse PNGs from the current session
        image_files = []
        for entry in os.scandir(self.timelapse_out_dir):
            if entry.is_file() and entry.name.startswith("timelapse_") and entry.name.endswith(".png"):
                try:
                    if entry.stat().st_mtime >= session_start_ts:
                        image_files.append(entry.path)
                except FileNotFoundError:
                    continue # File might have been deleted

        if len(image_files) < 2:
            logging.warning(f"Not enough timelapse images ({len(image_files)}) found for this session to create a video.")
            return

        # 2. Sort files chronologically (by filename, which contains the timestamp)
        image_files.sort()
        logging.info(f"Found {len(image_files)} timelapse images for video creation.")

        # 3. Create a temporary file list for FFmpeg (most reliable method)
        filelist_path = os.path.join(self.general_log_dir, "timelapse_filelist.txt")
        # TODO check to inject the image directly on the ffmpeg
        with open(filelist_path, 'w') as f:
            for path in image_files:
                # FFmpeg's concat demuxer needs a specific format
                f.write(f"file '{os.path.abspath(path)}'\n")

        # 4. Construct and run the FFmpeg command
        try:
            cfg = self.cfg["timelapse_video"]
            tstamp = self.session_start_time.strftime("%Y%m%d")
            output_path = os.path.join(self.timelapse_out_dir, f"timelapse_video_{tstamp}.mp4")

            ff_cmd = [
                "ffmpeg",
                "-y",                   # Overwrite output file if it exists
                "-f", "concat",         # Use the concatenation demuxer
                "-safe", "0",           # Allow absolute paths in the file list
                "-r", str(cfg["video_fps"]), # Input framerate (how fast to read images)
                "-i", filelist_path,    # The input file list
                "-c:v", cfg["ffmpeg_encoder"],
                "-b:v", cfg["ffmpeg_bitrate"],
                "-pix_fmt", "yuv420p",  # For broad player compatibility
                output_path
            ]
            
            logging.info(f"Encoding end-of-session timelapse video to {os.path.basename(output_path)}...")
            # subprocess.run(ff_cmd, check=True, capture_output=True, text=True)
            try:
                subprocess.run(ff_cmd, check=True, capture_output=True, text=True, timeout=600)
            except subprocess.TimeoutExpired:
                logging.error("Timelapse video creation timed out after 10 minutes. FFmpeg may be frozen.")
                self.log_health_event("ERROR", "FFMPEG_TIMEOUT", "Timelapse video encoding timed out.")
            
            
            logging.info(f"Successfully created timelapse video: {os.path.basename(output_path)}")

            # Also queue this final video for upload
            if self.sftp_uploader and self.running.is_set():
                logging.info(f"Queueing {os.path.basename(output_path)} for upload.")
                self.sftp_dispatch_q.put(output_path)

        except subprocess.CalledProcessError as e:
            logging.error("--- TIMELAPSE FFMPEG FAILED ---")
            logging.error("FFmpeg Command: %s", " ".join(e.cmd))
            logging.error("FFmpeg stderr:\n%s", e.stderr)
        except Exception as e:
            logging.exception("An error occurred during timelapse video creation: %s", e)
        finally:
            # 5. Clean up the temporary file list
            if os.path.exists(filelist_path):
                os.remove(filelist_path)
    # -----------------
    def save_event_metadata(self, event_path, event_frames):
        self._save_generic_event_metadata(event_path, event_frames)
    # -----------------
    def save_event_stack_metadata(self, img_path, event_frames, method_used, stack_info=None):
        self._save_generic_event_metadata(img_path, event_frames, stack_method=method_used, stack_info=stack_info)
    # -----------------
    def save_timelapse_metadata(self, img_path, method_used, frames_buffer, stack_info=None):
        if not frames_buffer: return

        # Calculate brightness from the first frame in the buffer
        first_frame = frames_buffer[0][1]
        gray = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY)
        # mean_brightness = np.mean(gray)
        mean_brightness = float(np.mean(gray)) / 256.0

        # Calculate the average threshold from all frames in the buffer
        thresholds = [t for (_, _, t) in frames_buffer if isinstance(t, (int, float))]
        avg_threshold = np.mean(thresholds) if thresholds else None

        feat_count = stack_info.get("stars", 0) if stack_info else 0
        # std_dev = stack_info.get("stddev", 0)
        # stddev_val = std_dev if std_dev != 0 else "N/A"
        stddev_val = stack_info.get("stddev") if stack_info else 0
        bias_list = stack_info.get("bias_reasons", []) if stack_info else []
        
        n_frames = len(frames_buffer)
        meta = self.get_metadata(n_frames=n_frames, mean_brightness=mean_brightness, effective_threshold=avg_threshold, stddev=stddev_val, bias_reasons=bias_list)
        
        meta["sky_status"] = self.last_sky_status.name if self.last_sky_status else "UNKNOWN"
        meta["stack_method"] = method_used 
        meta["alignment"] = "true" if method_used == "bias mode" else "false"
            
        json_path = img_path.rsplit('.', 1)[0] + ".json"
        self.save_json_atomic(json_path, meta)
        logging.info("Saved timelapse metadata to %s", json_path)
    # -----------------
    def get_metadata(self, n_frames=1, mean_brightness=None, effective_threshold=None, override_exposure=None, override_gain=None, alignment_features=None, stddev=None, bias_reasons=None, stack_info=None, **kwargs):
        #g = self.cfg["general"]
        # Overrides if provided (for snapshots), otherwise use live system state
        exposure_us = override_exposure if override_exposure is not None else self.current_exposure_us
        gain = override_gain if override_gain is not None else self.current_gain     

        if stack_info:
            stddev = stddev if stddev is not None else stack_info.get("stddev")
            alignment_features = alignment_features if alignment_features is not None else stack_info.get("stars")
            bias_reasons = bias_reasons if bias_reasons is not None else stack_info.get("bias_reasons")

        if mean_brightness:
            lux_val = self.calculate_lux(mean_brightness, exposure_us, gain)
        else:
            lux_val = 0.0

        equivalent_exposure_s = (exposure_us * n_frames) / 1e6
        meta = {
            "hostname": self.cfg["general"].get("hostname"),
            "ver": self.cfg["general"].get("version"),
            "location": self.cfg["general"].get("location"),
            "latitude": self.cfg["general"].get("latitude"),
            "longitude": self.cfg["general"].get("longitude"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "auto_exposure_tuning": self.cfg["capture"].get("auto_exposure_tuning"),
            "exposure_us": exposure_us,
            "frames_stacked": n_frames,
            "equivalent_exposure_s": equivalent_exposure_s,
            "gain": round(gain, 2),
            "red_gain": self.cfg["capture"].get("red_gain"),
            "blue_gain": self.cfg["capture"].get("blue_gain"),
            "lux": round(lux_val, 5),
        }
        if mean_brightness is not None:
            meta["mean_brightness"] = round(mean_brightness, 2)      

        if effective_threshold is not None:
            meta["effective_threshold"] = round(effective_threshold, 2)
        if alignment_features is not None:
            meta["alignment_features"] = alignment_features
        if stddev is not None: meta["sky_stddev"] = round(stddev, 3)
        if bias_reasons: meta["bias_reasons"] = bias_reasons
        
        if self.cfg["timelapse"].get("astro_stretch"): 
            meta["astro_stretch"] = "Enabled"
            meta["strength"] = self.cfg["timelapse"].get("astro_stretch_strength")
            
        return meta
    # -----------------
    def _save_generic_event_metadata(self, output_path, event_frames, stack_method=None, stack_info=None):
        if not event_frames: return

        # Calculate brightness from the first frame of the event
        first_frame = event_frames[0][1]
        gray = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY)
        # mean_brightness = np.mean(gray)
        mean_brightness = float(np.mean(gray)) / 256.0 

        # Calculate the average threshold from all frames in the event
        thresholds = [t for (_, _, t) in event_frames if isinstance(t, (int, float))]
        avg_threshold = np.mean(thresholds) if thresholds else None

        n_frames = len(event_frames)
        meta = self.get_metadata(n_frames=n_frames, mean_brightness=mean_brightness, effective_threshold=avg_threshold, stack_info=stack_info)
        
        meta["source_event_start_time"] = event_frames[0][0]
        meta["source_event_end_time"] = event_frames[-1][0]
        
        if stack_method:
            meta["stack_method"] = stack_method
            
        json_path = os.path.splitext(output_path)[0] + ".json"
        self.save_json_atomic(json_path, meta)
        logging.info("Saved event metadata to %s", json_path)
    # -----------------
    def _tally_data_write(self, file_path):
        """Adds the size of a newly written file to the session statistics."""
        try:
            if os.path.exists(file_path):
                size_bytes = os.path.getsize(file_path)
                size_mb = size_bytes / (1024 * 1024)
                with self.status_lock:
                    self.session_stats["data_written_mb"] += size_mb
        except Exception as e:
            logging.debug(f"Failed to tally file size for {file_path}: {e}")
    # -----------------
    # 7. DATA, LOGGING & NETWORKING
    # -----------------
    def event_logger_loop(self):
        """
        A dedicated thread that waits for event summary dictionaries and appends
        them as a new line in a structured CSV file.
        """
        log_cfg = self.cfg.get("event_log", {})
        os.makedirs(os.path.dirname(self.event_log_out_file), exist_ok=True)
        
        # Write the CSV header if the file is new or empty
        if not os.path.exists(self.event_log_out_file) or os.path.getsize(self.event_log_out_file) == 0:
            try:
                with open(self.event_log_out_file, "w") as f:
                    f.write("timestamp_utc,start_time_utc,duration_sec,num_frames_video,num_frames_stack\n")
            except IOError as e:
                logging.error("Could not write header to event log file '%s': %s", self.event_log_out_file, e)
                return

        logging.info("Event logger started. Will append summaries to %s", self.event_log_out_file)

        while True:
            try:
                # This is a blocking call, so the thread uses zero CPU while waiting
                log_data = self.event_log_q.get()

                # Check for the sentinel to exit
                if log_data is None:
                    logging.info("Event logger received sentinel. Exiting.")
                    break
                
                # Format the data into a CSV line
                line = (f"{log_data['timestamp_utc']},{log_data['start_time_utc']},"
                        f"{log_data['duration_sec']:.2f},{log_data['num_frames_video']},"
                        f"{log_data['num_frames_stack']}\n")

                with open(self.event_log_out_file, "a") as f:
                    f.write(line)

                # Increment and display the event counter
                self.event_counter += 1
                logging.info("Event #%d logged successfully. Total events captured: %d", self.event_counter, self.event_counter)

            except Exception as e:
                logging.exception("Error in event_logger_loop: %s", e)        
    # -----------------
    def health_monitor_loop(self):
        """
        A stateful worker that maintains a persistent counter of health events.
        It reads the stats file into memory, updates counts based on events from
        the health_q, and rewrites the file.
        """

        # --- In-memory state of the health statistics ---
        # Format: { "EVENT_TYPE": {"count": N, "level": "LEVEL", "last_message": "..."} }
        health_stats = {}

        # --- Read the existing stats file on startup ---
        try:
            if os.path.exists(self.health_log_out_file):
                with open(self.health_log_out_file, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Convert count back to an integer
                        health_stats[row['event_type']] = {
                            "count": int(row['count']),
                            "level": row['level'],
                            "last_message": row['last_message']
                        }
                logging.info(f"Health monitor loaded {len(health_stats)} existing event types.")
        except Exception as e:
            logging.error(f"Could not load existing health stats file: {e}")

        while self.running.is_set():
            try:
                # Wait for a new health event to arrive
                health_event = self.health_q.get(timeout=1.0)
                if health_event is None:
                    break # Sentinel for shutdown

                event_type = health_event['event_type']
                
                if event_type in health_stats:
                    # 1. If event type exists, increment the counter
                    health_stats[event_type]['count'] += 1
                    # Always update with the latest message and level
                    health_stats[event_type]['last_message'] = health_event['message']
                    health_stats[event_type]['level'] = health_event['level']
                else:
                    # 2. If it's a new event type, create a new entry
                    health_stats[event_type] = {
                        "count": 1,
                        "level": health_event['level'],
                        "last_message": health_event['message']
                    }
                
                try:
                    # Use a temporary file for an atomic write to prevent corruption
                    temp_path = self.health_log_out_file + ".tmp"
                    with open(temp_path, 'w', encoding='utf-8', newline='') as f:
                        # Define the fieldnames in the order you want them
                        fieldnames = ['event_type', 'count', 'level', 'last_message']
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        
                        writer.writeheader()
                        # Sort by count so the file is always nicely ordered
                        sorted_events = sorted(health_stats.items(), key=lambda item: item[1]['count'], reverse=True)
                        for event_type, data in sorted_events:
                            writer.writerow({
                                'event_type': event_type,
                                'count': data['count'],
                                'level': data['level'],
                                'last_message': data['last_message']
                            })
                    # Atomically replace the old file with the new one
                    os.rename(temp_path, self.health_log_out_file)
                except Exception as e:
                    logging.error(f"Failed to write updated health stats file: {e}")
                               
            except queue.Empty:
                continue # This is normal, just loop again
            except Exception as e:
                logging.exception("Error in health_monitor_loop: %s", e)
        
        logging.info("Health monitor has exited.")
    # -----------------
    def log_health_event(self, level, event_type, message):
        """Creates a structured health event and puts it on the health queue."""
        if not self.cfg.get("health_monitor", {}).get("enabled", True):
            return
        
        event = {
            "level": level,
            "event_type": event_type,
            "message": message.replace(",", ";") # Sanitize commas for CSV
        }
        try:
            self.health_q.put_nowait(event)
        except queue.Full:
            logging.warning("Health event queue is full. A statistic was dropped.")
    # -----------------
    def get_health_statistics(self, filepath):
        """Reads the pre-aggregated health stats CSV file and returns it as a list of dicts."""
        if not os.path.exists(filepath):
            return []
        
        try:
            with open(filepath, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                # The file is already sorted by count, so we just return the rows
                stats = list(reader)
            return stats
        except Exception as e:
            # Return an error that can be displayed on the dashboard
            return [{
                "event_type": "ERROR_READING_STATS",
                "count": 1, 
                "last_message": str(e), 
                "level": "ERROR"
            }]
    # -----------------
    def sftp_dispatcher_loop(self):
        """
        A smart dispatcher that acts as a persistent, disk-backed queue for the SFTP uploader.
        Implements the logic to use a disk file as a backlog when the memory queue is full.
        """
        if not self.sftp_uploader:
            logging.info("SFTP Dispatcher: Uploader is disabled. Thread will exit.")
            return

        logging.info(f"SFTP Dispatcher started. Using backlog file: {self.backlog_file_path}")

        while self.running.is_set():
            try:
                # 1. Check if we can flush from the disk backlog first.
                upload_q = self.sftp_uploader.upload_q
                
                if upload_q.qsize() < upload_q.maxsize and os.path.exists(self.backlog_file_path):
                    logging.info("SFTP Dispatcher: Upload queue has space. Flushing from disk backlog.")
                    temp_backlog = []
                    with open(self.backlog_file_path, 'r') as f:
                        temp_backlog = [line.strip() for line in f if line.strip()]
                    
                    files_flushed = 0
                    while temp_backlog and not upload_q.full():
                        file_path = temp_backlog.pop(0)
                        self.sftp_uploader.upload_q.put(file_path)
                        files_flushed += 1
                    
                    # Rewrite the backlog file with the remaining items
                    with open(self.backlog_file_path, 'w') as f:
                        for item in temp_backlog:
                            f.write(item + '\n')
                    
                    if not temp_backlog:
                        os.remove(self.backlog_file_path) # Clean up the file if it's empty
                    
                    logging.info(f"SFTP Dispatcher: Flushed {files_flushed} items from disk to memory queue.")

                # 2. Process new items from the main dispatch queue.
                try:
                    # Use a timeout to allow the loop to re-check the backlog file periodically
                    new_item = self.sftp_dispatch_q.get(timeout=5.0)
                    
                    # 3. Decide where to put the new item
                    if not self.sftp_uploader.upload_q.full():
                        # Rule 1: There's space, enqueue directly.
                        self.sftp_uploader.upload_q.put(new_item)
                    else:
                        # Rule 2: Queue is full, write to disk backlog.
                        logging.warning(f"SFTP Dispatcher: Upload queue is full. Writing {os.path.basename(new_item)} to disk backlog.")
                        with open(self.backlog_file_path, 'a') as f:
                            f.write(new_item + '\n')

                except queue.Empty:
                    # This is normal, just means no new files were created in the last 5 seconds.
                    # The loop will now restart and check the backlog file again.
                    continue

            except Exception as e:
                logging.exception(f"An error occurred in the SFTP Dispatcher loop: {e}")
                time.sleep(30) # Wait a bit before retrying on a major error
    # -----------------
    def sweep_and_enqueue_unuploaded(self):
        """Scans output directories on startup for files that were not yet uploaded."""
        logging.info("Performing startup sweep for un-uploaded files...")
        count = 0
        scan_dirs = [self.events_out_dir, self.timelapse_out_dir]
        for directory in scan_dirs:
            for entry in os.scandir(directory):
                if entry.is_file() and not entry.name.endswith('.json') and not entry.name.startswith('.'):
                    json_path = os.path.splitext(entry.path)[0] + ".json"
                    if os.path.exists(json_path):
                        try:
                            with open(json_path, 'r') as f:
                                meta = json.load(f)
                            if not meta.get("uploaded_at"):
                                self.sftp_dispatch_q.put(entry.path)
                                self.sftp_dispatch_q.put(json_path)
                                count += 2
                        except (json.JSONDecodeError, IOError):
                            logging.warning(f"Could not read metadata for {entry.name}, skipping upload check.")
                            self.log_health_event("WARNING", "METADATA_READ_ERROR", "Could not read metadata, skipping upload check.")
        if count > 0:
            logging.info(f"Found and enqueued {count} previously un-uploaded files.")
    # -----------------
    def janitor_loop(self):
        """A dedicated thread to periodically manage disk space and rotate logs."""
        logging.info("Janitor thread started. Will run checks every hour.")
       
        csv_path = self.monitor_out_file
        janitor_cfg = self.cfg.get("janitor", {})
        while self.running.is_set():
            
            csv_max_mb = self.cfg.get("janitor", {}).get("csv_rotation_mb", 20)
            csv_backups = self.cfg.get("janitor", {}).get("csv_backup_count", 5)
        
            # 1. Perform the multi-tiered disk space cleanup.
            # This function will return False if it fails to free enough space.
            cleanup_successful = self.manage_disk_space(janitor_cfg.get("threshold"), janitor_cfg.get("target"))

            # 2. If cleanup failed, it's a critical error. Stop the entire pipeline.
            if not cleanup_successful:
                if not self.emergency_stop_active.is_set():
                    logging.critical("!!! JANITOR FAILED TO FREE DISK SPACE. ENTERING EMERGENCY STOP. !!!")
                    logging.critical("All data acquisition will be halted to prevent a system crash.")
                    logging.critical("The dashboard remains active for manual intervention.")

                    # self.stop_producer_thread()
                    self.emergency_stop_active.set()

            # 3. If cleanup was successful, perform the data file rotation.
            self.rotate_data_file(csv_path, max_size_mb=csv_max_mb, backup_count=csv_backups)
            
            logging.info("Janitor run complete. Next check in 1 hour.")
            self.initial_janitor_check_complete.set()

            # if self.running.wait(timeout=3600):
                # break
            for _ in range(3600):
                if not self.running.is_set():
                    break
                time.sleep(1)
    # -----------------
    def manage_disk_space(self, threshold=90.0, target=75.0):
        """
        Performs a multi-tiered cleanup. Returns True on success, False on failure.
        Tier 1: Deletes leftover temporary event frame directories.
        Tier 2: Deletes the oldest timelapse files.
        """
        try:
            # 1. Get the path to monitor from the configuration.
            monitor_path = self.cfg["janitor"].get("monitor_path", "/")
            
            # 2. Use this path for all disk usage checks.
            disk = psutil.disk_usage(monitor_path)
            if disk.percent < threshold:
                return True # Nothing to do, success.

            logging.warning(f"Disk usage on '{monitor_path}' is at {disk.percent:.1f}% (threshold is {threshold:.1f}%). Starting prioritized cleanup.")
            self.log_health_event("WARNING", "DISK_USAGE_HIGH", f"Disk at {disk.percent:.1f}%")
            
            # --- TIER 1: CLEAN UP LEFTOVER EVENT BACKLOGS ---
            event_dir = self.events_out_dir
            backlog_count = 0
            if os.path.isdir(event_dir):
                with os.scandir(event_dir) as it:
                    for entry in it:
                        # Temporary directories are named 'event_..._frames'
                        if entry.is_dir() and entry.name.endswith('_frames'):
                            try:
                                logging.warning("Janitor: Found and deleting leftover event frame directory: %s", entry.name)
                                self.log_health_event("WARNING", "LEFTOVER_EVENT", "Found and deleting leftover event.")
                                shutil.rmtree(entry.path)
                                backlog_count += 1
                            except Exception:
                                logging.exception("Janitor: Error deleting backlog directory %s", entry.path)
            if backlog_count > 0:
                logging.info("Janitor: Tier 1 cleanup removed %d backlog directories.", backlog_count)
            
            # Re-check usage. If we're already good, we can stop.
            if psutil.disk_usage(monitor_path).percent < target:
                logging.info("Disk space is now sufficient. Cleanup complete.")
                return True

            # --- TIER 2: CLEAN UP OLDEST TIMELAPSE FILES ---
            # Map the config names to the actual directory paths
            dir_map = {
                "events": self.events_out_dir,
                "timelapse": self.timelapse_out_dir,
                "daylight": self.daylight_out_dir # Points to the diagnostics subfolder
                # Add other disposable directories here in the future
            }
            
            # Get the deletion order from the config
            delete_priority = self.cfg["janitor"].get("priority_delete", ["daylight", "timelapse", "events"])

            for tier_name in delete_priority:
                # Check disk space before starting each tier
                if psutil.disk_usage(monitor_path).percent < target:
                    logging.info(f"Disk space is now sufficient ({psutil.disk_usage(monitor_path).percent:.1f}%). Halting cleanup.")
                    break # Exit the priority loop
                                                           
                tier_path = dir_map.get(tier_name)
                if not tier_path or not os.path.isdir(tier_path):
                    continue

                logging.warning(f"Janitor: Disk space still critical. Deleting from Tier '{tier_name}'...")

                # Scan and sort all files in the current tier directory
                files_in_tier = [
                    entry for entry in os.scandir(tier_path) 
                    if entry.is_file() and not entry.name.startswith('.')
                ]
                files_in_tier.sort(key=lambda x: x.stat().st_mtime) # Sort by modification time, oldest first

                deleted_count_in_tier = 0
                for file_entry in files_in_tier:
                    # Check disk usage on every file to stop as soon as the target is met
                    if psutil.disk_usage(monitor_path).percent < target:
                        break # Exit the file loop for this tier
                    
                    try:
                        if self.safe_to_delete(file_entry.path):
                            logging.info(f"Janitor ({tier_name}): Deleting old, uploaded file: {file_entry.name}")
                            os.remove(file_entry.path)
                            json_path = os.path.splitext(file_entry.path)[0] + ".json"
                            if os.path.exists(json_path):
                                os.remove(json_path)
                            deleted_count_in_tier += 1
                        else:
                            logging.info(f"Janitor ({tier_name}): Skipping deletion of un-uploaded file: {file_entry.name}")
                    except OSError as e:
                        logging.error(f"Janitor: Failed to delete file {file_entry.path}: {e}")
                        self.log_health_event("ERROR", "JANITOR_DELETE_FAIL", f"Failed on {file_entry.name}")

                if deleted_count_in_tier > 0:
                    logging.info(f"Janitor: Tier '{tier_name}' cleanup removed {deleted_count_in_tier} file(s).")
            
            # --- FINAL CHECK ---
            final_disk_percent = psutil.disk_usage(monitor_path).percent
            if final_disk_percent < target:
                logging.info(f"Janitor: Cleanup successful. New disk usage on '{monitor_path}' is {final_disk_percent:.1f}%.")
                return True
            else:
                logging.critical(f"!!! JANITOR FAILURE: DISK SPACE ON '{monitor_path}' COULD NOT BE FREED SUFFICIENTLY ({final_disk_percent:.1f}%) !!!")
                return False

        except Exception:
            logging.exception("An unhandled error occurred in the janitor process.")
            return True # Return true to avoid an unnecessary emergency stop
    # -----------------
    def safe_to_delete(self, file_path):
        """Checks a file's metadata to see if it has been marked as uploaded."""
        # If SFTP is not enabled, it's always safe to delete.
        if not self.sftp_uploader:
            return True

        json_path = os.path.splitext(file_path)[0] + ".json"
        if not os.path.exists(json_path):
            # No metadata, assume it's an old file or an anomaly. Safer to keep.
            logging.warning(f"Janitor: Skipping deletion of {os.path.basename(file_path)} because it has no metadata.")
            self.log_health_event("WARNING", "JANITOR", "Skipping deletion no metadata.")
            return False

        try:
            with open(json_path, 'r') as f:
                meta = json.load(f)
        except Exception as e:
            logging.warning(f"Janitor: Failed to read metadata for {os.path.basename(file_path)}, skipping deletion: {e}")
            self.log_health_event("WARNING", "JANITOR", "Failed to read metadata, skipping deletion.")
            return False # Fail safe: don't delete if metadata is unreadable

        # Check for the 'uploaded_at' key.
        return "uploaded_at" in meta
    # -----------------
    def rotate_data_file(self, file_path, max_size_mb=10, backup_count=5):
        """Rotates a generic data file (like a CSV) if it exceeds a max size."""
        try:
            if not os.path.exists(file_path):
                return

            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

            if file_size_mb < max_size_mb:
                return

            logging.warning("Data file '%s' has reached %.1fMB. Rotating.", os.path.basename(file_path), file_size_mb)
            
            # --- Backup Rotation Logic ---
            # First, shift all existing backups: .4 -> .5, .3 -> .4, etc.
            for i in range(backup_count - 1, 0, -1):
                src = f"{file_path}.{i}"
                dst = f"{file_path}.{i+1}"
                if os.path.exists(src):
                    shutil.move(src, dst)
            
            # Now, rotate the main file to .1
            if os.path.exists(file_path):
                shutil.move(file_path, f"{file_path}.1")

            # CRITICAL: Re-create the header for the new, empty CSV file
            if file_path == self.monitor_out_file:
                 with open(file_path, "w") as f:
                    f.write("timestamp,cpu_temp,cpu_percent,mem_used_mb,mem_percent,"
                            "disk_used_mb,disk_percent,gpu_percent,load1,load5,load15,uptime_sec\n")

        except Exception:
            logging.exception("An error occurred during data file rotation.")
    # -----------------
    # 8. SYSTEM SERVICES (WATCHDOG & POWER)
    # -----------------
    def watchdog_loop(self, startup_delay_sec=10, check_interval_sec=5):
        """
        The master supervisor thread. It performs two critical functions:
        1. Periodically checks if all other critical threads are alive.
        2. Periodically pings the systemd watchdog to signal script health.

        If any thread has crashed, this loop will exit with an error code,
        triggering a service restart by systemd. If this loop itself freezes,
        systemd will stop receiving pings and restart the service.
        """
        try:
            import sdnotify
            n = sdnotify.SystemdNotifier()
            systemd_integration = True
        except ImportError:
            logging.warning("sdnotify library not found. Running without systemd integration.")
            systemd_integration = False

        # Give all other threads a moment to start up before signaling readiness.
        logging.info("Watchdog thread started. Waiting %d seconds before signaling READY.", startup_delay_sec)
        for _ in range(startup_delay_sec):
            if not self.running.is_set(): return
            time.sleep(1)

        # Signal systemd that the service is fully initialized and ready.
        if systemd_integration:
            n.notify("READY=1")
            logging.info("Watchdog Supervisor signaled READY=1 to systemd.")

        while self.running.is_set():
            if self.watchdog_pause.is_set():
                logging.warning("Watchdog checks are temporarily suspended for configuration reload.")
                # We still need to ping the systemd watchdog to prevent it from timing out
                if systemd_integration:
                    n.notify("WATCHDOG=1")
                # Wait for the interval and then restart the loop, skipping the thread checks
                time.sleep(check_interval_sec)
                continue
            
            # 1. Ping systemd to let it know the script is responsive.
            if systemd_integration:
                n.notify("WATCHDOG=1")

            # 2. Check the health of all other threads.
            all_monitored_threads = self.worker_threads + self.control_threads
            for thread in all_monitored_threads:
                if not thread.is_alive():
                    # --- EMERGENCY: A THREAD HAS CRASHED ---
                    msg = f"WATCHDOG: DETECTED CRASHED THREAD: {thread.name}"

                    self.log_health_event("CRITICAL", "THREAD_CRASH", msg)
                    # Stop the data producer.
                    self.stop_producer_thread()
                    # Set emergency mode
                    self.emergency_stop_active.set()
                                 
                    self.perform_system_action("exit", reason=msg)
            # If all is well, wait for the next check interval.
            for _ in range(check_interval_sec):
                if not self.running.is_set(): break
                time.sleep(1)
    # -----------------
    def maintenance_watchdog_loop(self):
        """
        A dedicated thread that monitors the maintenance mode timeout.
        If the timeout is reached, it automatically resumes the pipeline.
        """
        logging.info("Maintenance Watchdog started.")
        while self.running.is_set():
            timeout_target = 0
            with self.maintenance_timeout_lock:
                if self.maintenance_timeout_until > 0:
                    timeout_target = self.maintenance_timeout_until

            if timeout_target > 0:
                # A timeout is active, check if it has expired
                if time.time() >= timeout_target:
                    logging.warning("Maintenance mode timeout reached. Automatically resuming pipeline...")
                    self.in_maintenance_mode.clear()
                    # Reset the timer
                    with self.maintenance_timeout_lock:
                        self.maintenance_timeout_until = 0
                    
                    # Only resume if we are within the active schedule
                    if self.within_schedule():
                        self.start_producer_thread()
                    else:
                        logging.info("Maintenance timeout expired, but system is outside of active schedule. Staying idle.")
                
            # Sleep for a short interval before checking again
            # if self.running.wait(timeout=5):
                # break # Exit if shutdown is signaled
            for _ in range(5):
                if not self.running.is_set(): break
                time.sleep(1)
    # -----------------
    def system_monitor_loop(self):
        # Write the header if the file is new
        if not os.path.exists(self.monitor_out_file):
            with open(self.monitor_out_file, "w") as f:
                f.write("timestamp,cpu_temp,cpu_percent,mem_used_mb,mem_percent,"
                        "disk_used_mb,disk_percent,gpu_percent,load1,load5,load15,uptime_sec,core_voltage_v\n")

        while self.running.is_set():
            interval = self.cfg["monitor"].get("interval_sec", 5)
            ts = datetime.now(timezone.utc).isoformat()

            # temperatura CPU/SoC
            temps = psutil.sensors_temperatures()
            cpu_temp = temps.get("cpu_thermal", [None])[0].current if "cpu_thermal" in temps else 0
            self.session_stats["peak_cpu_temp"] = max(self.session_stats["peak_cpu_temp"], cpu_temp)

            # uso CPU
            cpu_percent = psutil.cpu_percent(interval=None)

            # memoria
            mem = psutil.virtual_memory()
            mem_used_mb = mem.used / (1024*1024)

            # disco
            disk = psutil.disk_usage("/")
            disk_used_mb = disk.used / (1024*1024)

            # GPU usage (se disponibile)
            gpu_percent = 0
            try:
                gpu_stat = subprocess.check_output(["vcgencmd", "measure_utilisation"], text=True)
                parts = dict(p.split("=") for p in gpu_stat.strip().split())
                if "gpu" in parts:
                    gpu_percent = parts["gpu"].replace("%", "")
            except Exception:
                gpu_percent = 0

            # load average
            load1, load5, load15 = os.getloadavg()

            # uptime sistema
            uptime_sec = time.time() - psutil.boot_time()
            
            core_voltage_v = self.get_core_voltage()            

            line = (f"{ts},{cpu_temp},{cpu_percent},{mem_used_mb:.1f},{mem.percent},"
                    f"{disk_used_mb:.1f},{disk.percent},{gpu_percent},{load1:.2f},"
                    f"{load5:.2f},{load15:.2f},{int(uptime_sec)},{core_voltage_v:.4f}\n")
            with open(self.monitor_out_file, "a") as f:
                f.write(line)

            # --- Trigger the full variable dump ---
            if self.cfg["general"].get("debug_level") == 4:
                self.publish_state_telemetry()

            time.sleep(interval)
    # -----------------
    def power_monitor_loop(self):
        '''
        Monitor a GPIO pin for power loss using the gpiod library.
        If a sustained outage is detected, it initiates
        a graceful shutdown and system halt.
        '''
        
        if not IS_GPIO_AVAILABLE:
            logging.warning("gpiod library not found. Power monitor is disabled.")
            return

        power_pin = self.cfg["power_monitor"].get("pin", 17)
        shutdown_delay_sec = self.cfg["power_monitor"].get("shutdown_delay_sec", 60)
        check_interval = 2  # Delay between check.
        
        line_request = None
        try:
            settings = gpiod.LineSettings(
                direction=gpiod.line.Direction.INPUT,
                bias=gpiod.line.Bias.PULL_UP
            )
            
            line_request = GPIO_CHIP.request_lines(
                consumer="meteora-pipeline-power",
                config={power_pin: settings}
            )
            logging.info(f"Power monitor started on GPIO pin {power_pin} (shutdown delay: {shutdown_delay_sec}s).")
        except Exception:
            logging.exception("Failed to initialize GPIO pin %d for power monitor. The thread will now exit.", power_pin)
            if line_request: line_request.release()
            return

        power_lost = False
        power_loss_time = 0

        while self.running.is_set():
            try:
                # Assumes power is OK when the pin is HIGH (1) and LOST when LOW (0).
                is_power_ok = line_request.get_value(power_pin) == gpiod.line.Value.ACTIVE
                              
                if not is_power_ok and not power_lost:
                    # Power outage
                    power_lost = True
                    power_loss_time = time.time()
                    with self.status_lock:
                        self.power_status = "LOST"
                    logging.warning("Initial power loss detected. Starting %d second confirmation timer.", shutdown_delay_sec)
                    self.log_health_event("WARNING", "POWER_ERROR", "Initial power loss detected")

                elif is_power_ok and power_lost:
                    # Power was restored.
                    power_lost = False
                    with self.status_lock:
                        self.power_status = "OK"
                    logging.info("Power was restored. Cancelling shutdown timer.")

                elif not is_power_ok and power_lost:
                    if time.time() - power_loss_time > shutdown_delay_sec:
                        msg="!!! CONFIRMED POWER OUTAGE (lost for >{shutdown_delay_sec} sec) !!!"
                        
                        with self.status_lock:
                            self.power_status = "SHUTTING DOWN"
                        self.perform_system_action("shutdown", reason=msg)
                        break 
                
                # if self.running.wait(timeout=check_interval):
                    # break
                time.sleep(check_interval)

            except Exception:
                logging.exception("An error occurred in the power monitor loop. The thread will exit.")
                break # Exit the loop on error
        
        if line_request:
            line_request.release()
    # -----------------
    def heartbeat_loop(self):
        """
        Periodically sends a heartbeat ping to a configured URL.
        Crucially, this thread runs INDEPENDENTLY of the main capture schedule
        to signal that the script process is alive at all times.
        """
        hb_cfg = self.cfg.get("heartbeat", {})

        url = hb_cfg.get("url")
        active_interval_min = hb_cfg.get("interval_min", 60)
        # Use a different, potentially longer interval for when the system is idle
        idle_interval_min = self.cfg["general"].get("idle_heartbeat_interval_min", 120) # e.g., 2 hours

        if not url:
            logging.warning("Heartbeat is enabled, but no valid URL is configured.")
            return
        
        try:
            import requests
        except ImportError:
            logging.error("'requests' library is required for the heartbeat feature. Please run 'pip3 install requests'.")
            return

        logging.info(f"Heartbeat monitor started. Will ping every {active_interval_min} min (active) or {idle_interval_min} min (idle).")

        while self.running.is_set():
            # 1. Determine the current state and wait interval
            is_active = self.producer_thread_active()
            current_interval_sec = (active_interval_min if is_active else idle_interval_min) * 60
            
            # Wait for the determined interval
            for _ in range(current_interval_sec):
                if not self.running.is_set(): break
                time.sleep(1)
            if not self.running.is_set(): break

            # 2. Send the ping
            try:
                # You can append a status to the URL for services that support it
                # For Healthchecks.io, the main URL is enough.
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                
                status_msg = "OK (Active)" if is_active else "OK (Idle)"
                with self.status_lock:
                    self.last_heartbeat_status = f"{status_msg} ({datetime.now().strftime('%H:%M:%S')})"
                logging.info(f"Heartbeat ping sent successfully (State: {'Active' if is_active else 'Idle'}).")

            except requests.exceptions.RequestException as e:
                with self.status_lock:
                    self.last_heartbeat_status = f"FAIL ({datetime.now().strftime('%H:%M:%S')})"
                logging.warning(f"Failed to send heartbeat ping: {e}")
                self.log_health_event("WARNING", "HEARTBEAT", "Failed to send heartbeat ping")
    # -----------------
    def ntp_sync_loop(self):
        """
        Periodically checks the system clock against an NTP server. If significant
        drift is detected, it triggers the OS's own time service to re-synchronize.
        """
        ntp_cfg = self.cfg.get("ntp", {})

        server = ntp_cfg.get("server", "pool.ntp.org")
        interval_sec = ntp_cfg.get("sync_interval_hours", 6) * 3600
        max_offset = ntp_cfg.get("max_offset_sec", 2.0)
        
        logging.info(f"NTP clock monitor started. Will check against '{server}' every {interval_sec / 3600} hours.")
        time.sleep(60) # Initial delay for network

        while self.running.is_set():
            try:
                
                server = ntp_cfg.get("server", "pool.ntp.org")
                interval_sec = ntp_cfg.get("sync_interval_hours", 6) * 3600
                max_offset = ntp_cfg.get("max_offset_sec", 2.0)                
                
                logging.info("Performing periodic NTP clock check...")
                client = ntplib.NTPClient()
                response = client.request(server, version=3, timeout=15)
                offset = response.offset
                logging.info(f"NTP check complete. System clock offset: {offset * 1000:.2f} ms.")

                if abs(offset) > max_offset:
                    logging.critical("="*60)
                    logging.critical("!!! CRITICAL CLOCK DRIFT DETECTED !!!")
                    logging.critical(f"System clock is off by {offset:.3f} seconds, which exceeds the threshold of {max_offset:.3f}s.")
                    logging.critical("Attempting to force a re-synchronization via the OS time service.")
                    logging.critical("="*60)
                    
                    try:
                        # This command tells systemd-timesyncd to re-enable and force a sync.
                        cmd = ["sudo", "/usr/bin/timedatectl", "set-ntp", "true"]
                        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=5)
                        logging.info("Successfully triggered OS time synchronization service.")
                        # After triggering, wait a bit for the sync to happen before the next check.
                        time.sleep(60)
                    except subprocess.TimeoutExpired:
                        logging.error("The ntp command has timed out.")
                    except subprocess.CalledProcessError as e:
                        logging.error("Failed to trigger OS time synchronization.")
                        self.log_health_event("ERROR", "NTP_FAIL", "Failed to trigger OS time synchronization.")
                        logging.error("Command failed: %s", " ".join(e.cmd))
                        logging.error("Stderr: %s", e.stderr)
                    except FileNotFoundError:
                        logging.error("Could not find '/usr/bin/timedatectl'. Cannot trigger time sync.")
                else:
                    logging.info("System clock is within acceptable tolerance.")

            except Exception as e:
                logging.warning(f"Could not complete NTP query against '{server}': {e}")
                self.log_health_event("WARNING", "NTP_ERROR", "Could not complete NTP query.")

            # Wait for the next interval
            logging.info(f"NTPThread is sleeping for {interval_sec / 3600:.1f} hours.")
            for _ in range(interval_sec):
                if not self.running.is_set(): break
                time.sleep(1)
    # -----------------
    def config_reloader_loop(self):
        """
        A dedicated thread that waits for a signal to reload the config file.
        It intelligently compares the new config to the old one, pauses the watchdog,
        and restarts critical threads if necessary.
        """
        logging.info("Smart configuration reloader thread started.")

        CRITICAL_CAPTURE_PARAMS = ["width", "height", "exposure_us", "gain", "auto_exposure", "red_gain", "blue_gain"]
        CRITICAL_POWER_PARAMS = ["pin"] # Pin for the power monitor

        last_cfg = json.loads(json.dumps(self.cfg)) 

        while self.running.is_set():
            if self.reload_config_signal.wait(timeout=10.0):
                try:
                    logging.warning("Reload signal received. Performing smart configuration reload.")
                    new_cfg = self.load_and_validate_config(self.config_path)

                    # --- Comparison Phase ---
                    requires_capture_restart = False
                    for param in CRITICAL_CAPTURE_PARAMS:
                        old_val = last_cfg.get("capture", {}).get(param)
                        new_val = new_cfg.get("capture", {}).get(param)
                        if old_val != new_val:
                            logging.critical(f"CRITICAL CHANGE DETECTED: 'capture.{param}' changed from '{old_val}' to '{new_val}'.")
                            requires_capture_restart = True
                            break
                    
                    power_pin_changed = False
                    for param in CRITICAL_POWER_PARAMS:
                        old_val = last_cfg.get("power_monitor", {}).get(param)
                        new_val = new_cfg.get("power_monitor", {}).get(param)
                        if old_val != new_val:
                            logging.critical(f"CRITICAL CHANGE DETECTED: 'power_monitor.{param}' changed from '{old_val}' to '{new_val}'.")
                            power_pin_changed = True
                            break

                    # --- Action Phase ---
                    if requires_capture_restart or power_pin_changed:
                        try:
                            if power_pin_changed:
                                msg="!!! Power monitor pin changed. A full service restart is required. !!!"
#                                logging.critical("="*60)
#                                logging.critical("!!! A CRITICAL PARAMETER HAS CHANGED. !!!")
#                                logging.critical("!!! A full service restart is required to apply this change safely. !!!")
#                                logging.critical("!!! The program restart automatically !!!")
#                                logging.critical("="*60)
                                self.perform_system_action("exit", reason=msg)
                                # The code will not proceed past this point.                           
                                                       
                            # --- PAUSE WATCHDOG ---
                            logging.warning("Pausing watchdog for thread restart procedure.")
                            self.watchdog_pause.set()
                            time.sleep(1) # Grace period

                            if requires_capture_restart:
                                logging.warning("Initiating graceful restart of the capture thread...")
                                self.stop_producer_thread()
                                time.sleep(1)
                                
                                # Apply new config BEFORE re-initializing camera
                                with self.status_lock:
                                    self.cfg = new_cfg

                                try:
                                    self.camera.release()
                                    self.camera = CameraCapture(self.cfg["capture"])
                                    logging.info("Camera re-initialized successfully with new settings.")
                                except Exception:
                                    logging.critical("Failed to re-initialize camera! This is a fatal error.", exc_info=True)
                                    self.perform_system_action("exit", reason="Fatal camera re-initialization failure")
                                
                                #self.start_producer_thread()

                        finally:
                            # --- RESUME WATCHDOG (CRITICAL) ---
                            logging.info("Resuming watchdog checks.")
                            self.watchdog_pause.clear()
                    
                    # If no restarts were needed, just apply the config live.
                    if not requires_capture_restart:
                        logging.info("No critical changes detected that require restarts. Applying new settings live.")
                        with self.status_lock:
                            self.cfg = new_cfg
                    
                    # Update last known config for the next comparison
                    last_cfg = json.loads(json.dumps(new_cfg)) 
                    logging.info("Smart configuration reload complete.")
                    #self.config_reloaded_ack.set()

                except Exception as e:
                    logging.exception("Failed to reload configuration: %s", e)
                finally:
                    self.reload_config_signal.clear()
                    self.config_reloaded_ack.set()
    # -----------------
    # 9. USER INTERFACE (DASHBOARD & LED)
    # -----------------
    def dashboard_loop(self):
        """A dedicated thread to run a Flask web server for status and control."""
        dash_cfg = self.cfg.get("dashboard", {})

        try:
            from flask import Flask, send_from_directory, redirect, url_for, request, render_template_string, Response, abort, jsonify
            from functools import wraps
            from datetime import timedelta
        except ImportError:
            logging.error("Flask library not found. 'pip3 install Flask'. Dashboard disabled.")
            return

        # 1. Read the token from the environment.
        DASH_TOKEN = os.environ.get("METEORA_DASH_TOKEN")

        # 2. If the token is not set or is empty, refuse to start.
        if not DASH_TOKEN:
            logging.critical("="*60)
            logging.critical("!!! SECURITY ALERT: METEORA_DASH_TOKEN is NOT SET. !!!")
            logging.critical("!!! All access to the dashboard will be DENIED as a security precaution. !!!")
            logging.critical("!!! To enable the dashboard, set this environment variable to a long, random string. !!!")
            logging.critical("="*60)
        else:
            logging.info("Dashboard authentication is ENABLED. Token has been loaded.")
         
        def require_token(f):
            @wraps(f)
            def inner(*args, **kwargs):
                # Rule 1: If the DASH_TOKEN was never configured on the server, deny all access.
                if not DASH_TOKEN:
                    abort(401) # Access Denied

                # Rule 2: If the token IS configured, the user must provide the correct one.
                token_header = request.headers.get("X-Auth-Token")
                token_query = request.args.get("token")

                if token_query == DASH_TOKEN or token_header == DASH_TOKEN:
                    return f(*args, **kwargs) # Access Granted
                
                # If we reach here, the token was wrong or missing.
                logging.warning(f"Dashboard: Unauthorized access attempt from {request.remote_addr}.")
                abort(401) # Access Denied
            return inner

        app = Flask(__name__, static_folder=os.path.abspath("output"))
#        CORS(app)  # Enable CORS for all routes
        
        # --- PARAMETER ---
        EDITABLE_PARAMS = {
            "capture": ["exposure_us", "gain", "red_gain", "blue_gain"],
            "timelapse": ["stack_N"],
        }
        TIMELAPSE_DIR = os.path.abspath(self.timelapse_out_dir)
        EVENTS_DIR = os.path.abspath(self.events_out_dir)
        ALLOWED_DIRS = [TIMELAPSE_DIR, EVENTS_DIR]
        
        # --- API Endpoints ---
        
        # --- START: SYSTEM TIME PAGE ---
        @app.route('/system_time', methods=['GET', 'POST'])
        @require_token
        def system_time():
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""
            
            if request.method == 'POST':
                try:
                    new_date = request.form.get('new_date')
                    new_time = request.form.get('new_time')
                    if not new_date or not new_time:
                        raise ValueError("Date and time fields cannot be empty.")

                    # Combine and format for the `date` command
                    datetime_str = f"{new_date} {new_time}"
                    logging.warning(f"Dashboard user is attempting to set system time to: {datetime_str}")
                    
                    # Securely execute the command
                    cmd = ["sudo", "date", "-s", datetime_str]
                    result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=10)
                    
                    logging.info(f"System time successfully set. Command output: {result.stdout.strip()}")
                    return redirect(url_for('system_time', token=DASH_TOKEN, success='true'))
                
                except (ValueError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                    error_message = str(e)
                    if hasattr(e, 'stderr'):
                        error_message = e.stderr.strip()
                    logging.error(f"Failed to set system time: {error_message}")
                    # URL-encode the error message for safe transport
                    from urllib.parse import quote
                    return redirect(url_for('system_time', token=DASH_TOKEN, error=quote(error_message)))

            # Handle GET request (display the page)
            now = datetime.now()
            current_date = now.strftime('%Y-%m-%d')
            current_time = now.strftime('%H:%M:%S')
            
            feedback_html = ""
            if request.args.get('success'):
                feedback_html = '<div class="banner-success">System time updated successfully!</div>'
            elif request.args.get('error'):
                error_msg = html.escape(request.args.get('error'))
                feedback_html = f'<div class="banner-error"><strong>Error:</strong> {error_msg}<br><small>Ensure /bin/date is in your sudoers file.</small></div>'
                
            html_page = f"""
            <!DOCTYPE html><html lang="en">
            <head>
              <meta charset="UTF-8"><title>Set System Time - Meteora</title>
              <style>
                body {{ font-family: 'Segoe UI', sans-serif; background: #1e1e1e; color: #e0e0e0; margin: 0; padding: 0; }}
                header {{ background: #2c2c2c; padding: 1em 2em; border-bottom: 1px solid #444; }}
                h1 {{ margin: 0; color: #61afef; }}
                .main {{ padding: 2em; max-width: 900px; margin: auto; }}
                .card {{ background: #2b2b2b; border-radius: 12px; padding: 1.5em; box-shadow: 0 2px 6px rgba(0,0,0,0.5); }}
                label {{ font-weight: bold; display: block; margin-bottom: 5px; }}
                input[type=date], input[type=time] {{ background: #1a1a1a; border: 1px solid #444; color: #e0e0e0; padding: 10px; border-radius: 4px; font-size: 1.1em; margin-bottom: 1em; }}
                button {{ background: #e5c07b; color: #1e1e1e; font-weight: bold; border: none; padding: 12px 20px; border-radius: 8px; cursor: pointer; font-size: 1em; margin-top: 1em; }}
                button:hover {{ background: #ffd791; }}
                a {{ color: #c678dd; }}
                .banner-success {{ background-color: #2c5f2d; color: #fff; padding: 1em; border-radius: 8px; margin-bottom: 1em; text-align: center; }}
                .banner-error {{ background-color: #8b0000; color: #fff; padding: 1em; border-radius: 8px; margin-bottom: 1em; }}
              </style>
            </head>
            <body>
              <header><h1>Set System Time</h1></header>
              <div class="main">
                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                {feedback_html}
                <div class="card">
                  <h2>Current System Time: {current_date} {current_time}</h2>
                  <p style="color: #e5c07b;">Use this page to manually correct the system clock if the device has booted without an internet connection for NTP sync.</p>
                  <form method="post" action="/system_time{token_param}" onsubmit="return confirm('Are you sure you want to change the system time? This can affect logging and file timestamps.');">
                    <div>
                        <label for="new_date">New Date:</label>
                        <input type="date" id="new_date" name="new_date" value="{current_date}">
                    </div>
                    <div>
                        <label for="new_time">New Time (UTC):</label>
                        <input type="time" id="new_time" name="new_time" value="{current_time}" step="1">
                    </div>
                    <button type="submit">Set New Time</button>
                  </form>
                </div>
              </div>
            </body></html>
            """
            return render_template_string(html_page)

        # --- END: SYSTEM TIME PAGE ---
        
        # --- Pause the acquisition.
        @app.route('/api/pause')
        @require_token
        def pause_capture():
            logging.warning("Pause command received from dashboard. Stopping worker threads.")
            self.stop_producer_thread()
            self.in_maintenance_mode.set()
            with self.maintenance_timeout_lock:
                self.maintenance_timeout_until = time.time() + self.maintenance_timeout_duration_sec            
            return redirect(url_for('dashboard', token=DASH_TOKEN))
        
        # --- Resume the acquisition.
        @app.route('/api/resume')
        @require_token
        def resume_capture():
            logging.warning("Resume command received from dashboard. Restarting worker threads.")
#            self.start_producer_thread()
            self.in_maintenance_mode.clear()
            with self.maintenance_timeout_lock:
                self.maintenance_timeout_until = 0  

            if self.within_schedule():
                logging.warning("Resume command received from dashboard. Restarting worker threads as we are within schedule.")
                self.start_producer_thread()
                # Redirect with a success message
                return redirect(url_for('dashboard', token=DASH_TOKEN, resume_status='success'))
            else:
                logging.warning("Resume command received, but system is outside of active schedule. "
                              "Staying idle and letting the scheduler take over at the next active window.")
                # Redirect with an informational message
                return redirect(url_for('dashboard', token=DASH_TOKEN, resume_status='ignored'))
        
        # --- Show EDITABLE_PARAMS, save and test for image test 
        @app.route('/api/save_and_test', methods=['POST'])
        @require_token
        def save_and_test():
            logging.info("Dashboard: Received /api/save_and_test request.")
            if self.producer_thread_active():
                logging.warning("Dashboard: Capture threads ARE active, skipping test shot trigger.")
                return redirect(url_for('dashboard', token=DASH_TOKEN))

            try:
                # --- Stage 1: Clear old status and save new config ---
                with self.status_lock:
                    self.last_calibration_error = None
                    self.last_calibration_image_path = None
                
                with self.maintenance_timeout_lock:
                    self.maintenance_timeout_until = time.time() + self.maintenance_timeout_duration_sec
                
                logging.info("Dashboard: Saving new configuration from dashboard.")
                
                changes_from_form = {}
                for section, params in EDITABLE_PARAMS.items():
                    for param in params:
                        form_key = f"{section}_{param}"
                        if form_key in request.form:
                            if section not in changes_from_form:
                                changes_from_form[section] = {}
                            changes_from_form[section][param] = request.form[form_key]

                try:
                    validated_changes = MainConfig.model_validate(changes_from_form).model_dump(exclude_unset=True)
                except Exception as e:
                    logging.error(f"Dashboard: New configuration failed validation: {e}")
                    return redirect(url_for('dashboard', token=DASH_TOKEN))
                
                config_on_disk = self.legacy_load_config(self.config_path, merge=False)
                if not isinstance(config_on_disk, dict): config_on_disk = {}

                def recursive_update(d, u):
                    for k, v in u.items():
                        if isinstance(v, dict):
                            d[k] = recursive_update(d.get(k, {}), v)
                        else:
                            d[k] = v
                    return d
                
                config_to_save = recursive_update(config_on_disk, validated_changes)
                self.save_json_atomic(self.config_path, config_to_save)
                
                # --- Stage 2: Trigger and WAIT for the reloader to finish ---
                logging.info("Dashboard: Triggering config reloader and waiting for completion...")
                self.config_reloaded_ack.clear() # Ensure ack is cleared before signaling
                self.reload_config_signal.set()
                
                # Wait for a longer, more realistic timeout.
                if not self.config_reloaded_ack.wait(timeout=20.0):
                    logging.error("Dashboard: Timed out waiting for config reloader ack.")
                    with self.status_lock:
                        self.last_calibration_error = "Timed out waiting for config reload."
                    return redirect(url_for('dashboard', token=DASH_TOKEN))
                    
                self.config_reloaded_ack.clear()
                logging.info("Dashboard: Config reloader acknowledged completion.")
                
                
                # --- Stage 3: Start calibration process ONLY after reload is complete ---
                if self.is_calibrating.is_set():
                    logging.warning("Dashboard: Calibration is already in progress.")
                else:
                    logging.info("Dashboard: Setting calibration mode and starting producer thread...")
                    self.is_calibrating.set()
                    self.start_producer_thread()

            except Exception as e:
                logging.exception("A critical error occurred while saving the configuration: %s", e)
                with self.status_lock:
                    self.last_calibration_error = f"An unexpected error occurred: {e}"

            return redirect(url_for('dashboard', token=DASH_TOKEN))

        @app.route('/calibration_image')
        @require_token
        # --- The image coming from the calibration settings
        def serve_calibration_image():
            with self.status_lock:
                path = self.last_calibration_image_path
            if path and os.path.exists(path):
                return send_from_directory(os.path.dirname(path), os.path.basename(path))
            return "No image available.", 404

        @app.route('/files/action', methods=['POST'])
        @require_token
        # --- Download and delete file from files page
        def file_action():
            selected_files = request.form.getlist('selected_files')
            action = request.form.get('action')

            if not selected_files:
                logging.warning("File action requested but no files were selected.")
                return redirect(url_for('file_manager', token=DASH_TOKEN))

            # --- SECURITY VALIDATION ---
            for f_path in selected_files:
                if not self.is_safe_path(ALLOWED_DIRS, f_path):
                    logging.error("SECURITY ALERT: Attempted action on unsafe path: %s", f_path)
                    return "Forbidden: Path is outside of allowed directories.", 403

            if action == 'delete':
                logging.warning("User initiated deletion of %d file(s) from dashboard.", len(selected_files))
                deleted_count = 0
                for f_path in selected_files:
                    try:
                        os.remove(f_path)
                        # Also try to remove associated .json file
                        json_path = os.path.splitext(f_path)[0] + ".json"
                        if os.path.exists(json_path):
                            os.remove(json_path)
                        deleted_count += 1
                    except OSError as e:
                        logging.error("Failed to delete file %s: %s", f_path, e)
                logging.info("Successfully deleted %d file(s).", deleted_count)
                return redirect(url_for('file_manager', token=DASH_TOKEN))

            elif action == 'download':
                if len(selected_files) == 1:
                    # Download a single file directly
                    f_path = selected_files[0]
                    try:
                        directory = os.path.dirname(f_path)
                        filename = os.path.basename(f_path)
                        return send_from_directory(directory, filename, as_attachment=True)
                    except FileNotFoundError:
                        logging.error("Attempted to download a file that does not exist: %s", f_path)
                        return "File not found.", 404                        
                else:
                    # Zip multiple files and download
                    memory_file = io.BytesIO()
                    with ZipFile(memory_file, 'w') as zipf:
                        for f_path in selected_files:
                            try:
                                # This operation is now protected.
                                if os.path.exists(f_path):
                                    zipf.write(f_path, arcname=os.path.basename(f_path))
                                else:
                                    logging.warning("Skipping missing file during zip creation: %s", f_path)
                            except Exception as e:
                                # Catch any other file-related errors (e.g., permissions)
                                logging.error("Error adding file to zip archive %s: %s", f_path, e)
                    memory_file.seek(0)
                    
                    # If we added at least one file, send the zip. Otherwise, redirect.
                    if len(zipf.infolist()) > 0:
                        return Response(
                            memory_file,
                            mimetype='application/zip',
                            headers={'Content-Disposition': 'attachment;filename=meteora_download.zip'}
                        )
                    else:
                        logging.warning("Multi-file download requested, but no valid files could be zipped.")
                        return redirect(url_for('file_manager', token=DASH_TOKEN))
            
            return "Invalid action.", 400

        @app.route('/api/restart_service', methods=['POST'])
        @require_token
        def restart_service():
            logging.warning("User-initiated restart command received from dashboard.")
            
            # Use a separate thread to give Flask time to return a response to the user's browser
            # before the main process exits.
            def delayed_restart():
                time.sleep(1)
                self.perform_system_action("exit", reason="User-initiated restart from dashboard")
            
            threading.Thread(target=delayed_restart).start()
            
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""
            refresh_url = url_for('dashboard', _external=True) + token_param

            html_response = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Restarting Service...</title>
                <meta http-equiv="refresh" content="5;url={refresh_url}">
                <style>
                    body {{ font-family: 'Segoe UI', sans-serif; background: #1e1e1e; color: #e0e0e0; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
                    .container {{ text-align: center; padding: 2em; background: #2b2b2b; border-radius: 12px; border: 1px solid #3a3d54; }}
                    h1 {{ color: #e5c07b; }}
                    a {{ color: #8be9fd; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Restart Initiated</h1>
                    <p>The pipeline service is now restarting. This may take a moment.</p>
                    <p>You will be redirected to the main dashboard in 5 seconds.</p>
                    <p>(If you are not redirected, <a href="{refresh_url}">click here</a>.)</p>
                </div>
            </body>
            </html>
            """
            return html_response

        @app.route('/api/reboot_system', methods=['POST'])
        @require_token
        def reboot_system():
            logging.warning("User-initiated SYSTEM REBOOT command received from dashboard.")
            
            def delayed_reboot():
                time.sleep(2)
                # This calls the existing system logic for OS reboot
                self.perform_system_action("reboot", reason="User-initiated reboot from dashboard")
            
            threading.Thread(target=delayed_reboot).start()
            
            return """
            <html><body style="background:#1e1e1e;color:#e06c75;text-align:center;padding-top:100px;font-family:sans-serif;">
                <h1>System Rebooting...</h1>
                <p>The Raspberry Pi is restarting. This will take about 60 seconds.</p>
            </body></html>
            """

        # --- Settings page ---
        @app.route('/api/save_settings', methods=['POST'])
        @require_token
        def save_settings():
            logging.warning("Received request to save full configuration from dashboard.")
            try:
                # --- START: NEW, CORRECTED LOGIC ---

                # 1. Build a dictionary from the multi-part form data.
                # This correctly handles checkboxes by taking the last value ('true' if checked).
                form_dict = {}
                for key in request.form.keys():
                    parts = key.split('.')
                    d = form_dict
                    for part in parts[:-1]:
                        d = d.setdefault(part, {})
                    # For checkboxes, request.form.getlist(key) will be ['false', 'true'].
                    # We take the last one, which is the correct state.
                    d[parts[-1]] = request.form.getlist(key)[-1]

                # 2. Validate the data from the form. Pydantic will handle type coercion.
                try:
                    validated_model = MainConfig.model_validate(form_dict)
                    final_cfg = validated_model.model_dump()
                except Exception as e:
                    logging.error(f"New configuration failed validation: {e}")
                    return f"Configuration validation failed. Check logs. <a href='/settings?token={DASH_TOKEN}'>Go back</a>"

                # 3. Load the user's original config file to use as a structural template.
                config_to_save = self.legacy_load_config(self.config_path, merge=False)
                if not isinstance(config_to_save, dict): config_to_save = {}
                
                # 4. Recursively update the template with new values from the validated form data.
                #    This preserves the exact structure of the user's original config file.
                def update_existing_keys(original_dict, new_full_dict):
                    for key, value in original_dict.items():
                        if key in new_full_dict:
                            if isinstance(value, dict) and isinstance(new_full_dict.get(key), dict):
                                # Recurse into nested dictionaries
                                update_existing_keys(value, new_full_dict[key])
                            else:
                                # Update the value in the original dictionary
                                original_dict[key] = new_full_dict[key]

                update_existing_keys(config_to_save, final_cfg)
                
                # 5. Save the updated configuration and trigger a reload.
                self.save_json_atomic(self.config_path, config_to_save)
                self.reload_config_signal.set()
                self.config_reloaded_ack.wait(timeout=3.0)
                self.config_reloaded_ack.clear()
                logging.info("Successfully saved updated configuration file and reloaded.")
                return redirect(url_for('settings', token=DASH_TOKEN, save_success='true'))

            except Exception as e:
                logging.exception("A critical error occurred in /api/save_settings: %s", e)
                return f"An internal error occurred. <a href='/settings?token={DASH_TOKEN}'>Go back</a>"
                
        def generate_form_fields(data, prefix=''):
            html_out = ""
            # EDITABLE_BOOLEANS = ["auto_exposure_tuning", "auto_sensitivity_tuning", "stack_align", "astro_stretch"]
            # Iterate in dictionary order, to match DEFAULTS structure
            for key, value in data.items():
                current_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(value, dict):
                    html_out += f'<tr><th colspan="2" style="padding-top: 1em;">{key.replace("_", " ").upper()}</th></tr>'
                    html_out += generate_form_fields(value, prefix=current_prefix)
                else:
                    if isinstance(value, bool):
                        is_editable = key in self.editable_booleans
                        
                        if is_editable:
                            # This checkbox is user-editable
                            # label_cell = f'<td><label for="{current_prefix}">{key}</label></td>'
                            label_cell = f'<td><label class="editable-label" for="{current_prefix}">{key}</label></td>'
                            input_cell = f'''
                            <td>
                                <input type="hidden" name="{current_prefix}" value="false">
                                <input type="checkbox" id="{current_prefix}" name="{current_prefix}" value="true" {"checked" if value else ""}>
                            </td>
                            '''
                        else:
                            # For booleans (read-only), generate a label WITHOUT the 'for' attribute
                            # This severs the link that allows the label to toggle the checkbox
                            # for future use, actually yhe checkboxes are disabled
                            label_cell = f'<td><label>{key}</label></td>'
                            input_cell = f'''
                            <td class="readonly-cell">
                                <input type="hidden" name="{current_prefix}" value="false">
                                <input type="checkbox" id="{current_prefix}" name="{current_prefix}" value="true" {"checked" if value else ""}>
                            </td>
                            '''
                        html_out += f'<tr>{label_cell}{input_cell}</tr>'
                    else:
                        # For all other editable types, keep the 'for' attribute for better UX
                        label_cell = f'<td><label for="{current_prefix}">{key}</label></td>'
                        val_str = html.escape(str(value if value is not None else ""))
                        input_cell = f'<td><input type="text" id="{current_prefix}" name="{current_prefix}" value="{val_str}" size="40"></td>'
                        html_out += f'<tr>{label_cell}{input_cell}</tr>'
            return html_out

        @app.route('/settings')
        @require_token
        def settings():
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""
            
            current_merged_config = self.load_and_validate_config(self.config_path, merge=False)
            form_fields = generate_form_fields(current_merged_config)

            save_success_banner = ""
            if request.args.get('save_success') == 'true':
                save_success_banner = '<div class="banner-success">Configuration saved and reloaded successfully!</div>'

            html_page = f"""
            <!DOCTYPE html><html lang="en">
            <head>
              <meta charset="UTF-8"><title>Configuration Editor</title>
              <style>
                body {{ font-family: 'Segoe UI', sans-serif; background: #1e1e1e; color: #e0e0e0; margin: 0; padding: 0; }}
                header {{ background: #2c2c2c; padding: 1em 2em; border-bottom: 1px solid #444; }}
                h1 {{ margin: 0; color: #61afef; }}
                .main {{ padding: 2em; max-width: 900px; margin: auto; }}
                .card {{ background: #2b2b2b; border-radius: 12px; padding: 1.5em; box-shadow: 0 2px 6px rgba(0,0,0,0.5); }}
                table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
                td, th {{ padding: 8px 10px; border-bottom: 1px solid #444; text-align: left; }}
                th {{ color: #98c379; background: #2c2c2c; }}
                label {{ font-weight: bold; }}
                input[type=text] {{ background: #1a1a1a; border: 1px solid #444; color: #e0e0e0; padding: 6px; border-radius: 4px; width: 90%; }}
                input[type=checkbox] {{ transform: scale(1.3); }}
                button {{ background: #61afef; color: #fff; border: none; padding: 12px 20px; border-radius: 8px; cursor: pointer; font-size: 1em; margin-top: 1em; }}
                button:hover {{ background: #4fa3d8; }}
                a {{ color: #c678dd; }}
                .banner-success {{ background-color: #2c5f2d; color: #97f097; padding: 1em; border-radius: 8px; margin-bottom: 1em; text-align: center; }}
                .readonly-cell {{ pointer-events: none; opacity: 0.6; }}
                .editable-label {{ color: #e5c07b; }}
                .btn-danger {{ background-color: #e06c75; }}
                .btn-danger:hover {{ background-color: #c65c66; }}
                .button-group {{ display: flex; gap: 10px; align-items: center; }}
                .btn-warn {{ background-color: #e5c07b; color: #1e1e1e; }}
                .btn-warn:hover {{ background-color: #f0d5a0; }}
                .btn-system {{ background-color: #be5046; color: #fff; }}
                .btn-system:hover {{ background-color: #e06c75; }}
                .admin-section {{ border-top: 1px solid #444; margin-top: 30px; padding-top: 20px; }}
              </style>
            </head>
            <body>
              <header><h1>Configuration Editor</h1></header>
              <div class="main">
                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                {save_success_banner}
                <div class="card">
                  <p style="color: #ccc;">This is the active configuration.</p>
                  <p style="color: #e5c07b;"><b>Note:</b> Editable checkbox are highlighted in yellow.</p>
                  <form action="/api/save_settings{token_param}" method="post">
                    <table>{form_fields}</table>
                    <div class="button-group">
                        <button type="submit">Save and Reload Configuration</button>
                    </div>
                  </form>             
                  <!-- NEW ADMIN SECTION -->
                  <div class="admin-section">
                    <h2 style="color: #e06c75; font-size: 1.1em;">System Administration</h2>
                    <p style="color:#ccc;">If significant changes are made, a full restart is recommended.</p>
                    <div class="button-group" style="display: flex; gap: 15px; flex-wrap: wrap;">
                        
                        <!-- RESTART PROGRAM -->
                        <form action="/api/restart_service{token_param}" method="post" 
                              onsubmit="return confirm('Restart the Meteora Pipeline script? (This will interrupt active captures)');">
                            <button type="submit" class="btn-warn">Restart Program Only</button>
                        </form>

                        <!-- RESTART SYSTEM -->
                        <form action="/api/reboot_system{token_param}" method="post" 
                              onsubmit="return confirm('REBOOT the entire Raspberry Pi? (Connection will be lost)');">
                            <button type="submit" class="btn-system">Restart Full System</button>
                        </form>

                    </div>
                    <p style="font-size: 0.8em; color: #888; margin-top: 10px;">
                        Note: Restarting the system takes longer than restarting the program.
                    </p>
                  </div>
                </div>
                

                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                </div>
              </div>
            </body></html>
            """
            return render_template_string(html_page)

        @app.route('/files')
        @require_token
        # --- Files page
        def file_manager():
            event_files = self.get_file_list(EVENTS_DIR)
            timelapse_files = self.get_file_list(TIMELAPSE_DIR)
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""
            
            def generate_table_rows(files):
                rows = ""
                for f in files:
                    rows += f"""
                    <tr>
                        <td><input type="checkbox" name="selected_files" value="{f['path']}"></td>
                        <td>{f['name']}</td>
                        <td>{f['size_mb']}</td>
                        <td>{f['mtime']}</td>
                    </tr>
                    """
                return rows

            html_page = f"""
            <!DOCTYPE html><html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>File Manager - Meteora Pipeline</title>
              <style>
                body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #1e1e1e; color: #e0e0e0; margin: 0; padding: 0; }}
                header {{ background: #2c2c2c; padding: 1em 2em; border-bottom: 1px solid #444; }}
                h1 {{ margin: 0; color: #61afef; }}
                .main {{ padding: 2em; display: grid; grid-template-columns: 1fr; gap: 20px; }}
                .card {{ background: #2b2b2b; border-radius: 12px; padding: 1.5em; box-shadow: 0 2px 6px rgba(0,0,0,0.5); }}
                .card h2 {{ margin-top: 0; font-size: 1.2em; color: #98c379; }}
                table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; margin-top: 1em; }}
                th, td {{ padding: 8px 10px; border-bottom: 1px solid #444; }}
                th {{ text-align: left; color: #61afef; background: #2c2c2c; }}
                .ok {{ color: #98c379; font-weight: bold; }}
                .warn {{ color: #e5c07b; font-weight: bold; }}
                .err {{ color: #e06c75; font-weight: bold; }}
                input[type=checkbox] {{ transform: scale(1.2); }}
                button {{ background: #61afef; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; font-size: 0.9em; margin: 10px 5px 0 0; transition: 0.2s; }}
                button:hover {{ background: #4fa3d8; }}
                .btn-danger {{ background: #e06c75; }}
                .btn-danger:hover {{ background: #c65c66; }}
                a {{ color: #c678dd; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
              </style>
            </head>
            <body>
              <header>
                <h1>File Manager</h1>
              </header>
              <div class="main">
                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                <form action="/files/action{token_param}" method="post">
                  <div class="card">
                    <h2>Events ({len(event_files)} files)</h2>
                    <table>
                      <thead>
                        <tr><th>Select</th><th>Filename</th><th>Size (MB)</th><th>Date Modified</th></tr>
                      </thead>
                      <tbody>{generate_table_rows(event_files)}</tbody>
                    </table>
                  </div>
                  <div class="card">
                    <h2>Timelapse ({len(timelapse_files)} files)</h2>
                    <table>
                      <thead>
                        <tr><th>Select</th><th>Filename</th><th>Size (MB)</th><th>Date Modified</th></tr>
                      </thead>
                      <tbody>{generate_table_rows(timelapse_files)}</tbody>
                    </table>
                  </div>
                  <div>
                    <button type="submit" name="action" value="download">Download Selected</button>
                    <button type="submit" name="action" value="delete" class="btn-danger" onclick="return confirm('Are you sure you want to permanently delete the selected files?');">Delete Selected</button>
                    <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                  </div>
                </form>
              </div>
            </body>
            </html>
            """

            return render_template_string(html_page)

        @app.route('/latest_timelapse_image')
        @require_token
        # --- The last timelapse image ---
        def serve_latest_timelapse_image():
            # This logic is safe because it only ever serves from one specific directory
            latest_image_path = self.get_latest_file(TIMELAPSE_DIR)
            if latest_image_path and os.path.exists(latest_image_path):
                return send_from_directory(os.path.dirname(latest_image_path), os.path.basename(latest_image_path))
            # Return a 404 if no image is found
            return "No timelapse image available.", 404

    # ------ Event Statistics
        @app.route('/api/event_stats')
        @require_token
        def event_stats():
            """API endpoint to read and process events.csv for charting."""
            stats = {
                "by_hour": {str(h): 0 for h in range(24)},
                "by_night": {}
            }
            if not os.path.exists(self.event_log_out_file):
                return jsonify(stats) # Return empty stats if file doesn't exist

            try:
                with open(self.event_log_out_file, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Parse the UTC timestamp
                            ts_str = row['timestamp_utc'].replace('Z', '+00:00')
                            dt_utc = datetime.fromisoformat(ts_str)
                            
                            # Aggregate by hour
                            hour_key = str(dt_utc.hour)
                            stats["by_hour"][hour_key] += 1
                            
                            # Aggregate by "night" (noon to noon)
                            # If it's before noon, it belongs to the previous day's "night".
                            if dt_utc.hour < 12:
                                night_date = (dt_utc - timedelta(days=1)).strftime('%Y-%m-%d')
                            else:
                                night_date = dt_utc.strftime('%Y-%m-%d')
                            
                            stats["by_night"][night_date] = stats["by_night"].get(night_date, 0) + 1
                            
                        except (ValueError, KeyError):
                            continue # Skip malformed rows
                
                return jsonify(stats)
            except Exception as e:
                logging.error(f"Failed to process event stats: {e}")
                return jsonify({"error": "Failed to process event log"}), 500

        @app.route('/stats')
        @require_token
        def stats_page():
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""
            html_page = f"""
            <!DOCTYPE html><html lang="en">
            <head>
              <meta charset="UTF-8"><title>Event Statistics - Meteora</title>
              <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
              <style>
                body {{ font-family: 'Segoe UI', sans-serif; background: #1e1e1e; color: #e0e0e0; margin: 0; padding: 0; }}
                header {{ background: #2c2c2c; padding: 1em 2em; border-bottom: 1px solid #444; }}
                h1, h2 {{ margin: 0; color: #61afef; }}
                .main {{ padding: 2em; max-width: 1200px; margin: auto; }}
                .chart-container {{ background: #2b2b2b; border-radius: 12px; padding: 1.5em; box-shadow: 0 2px 6px rgba(0,0,0,0.5); margin-top: 2em; }}
                a {{ color: #c678dd; }}
              </style>
            </head>
            <body>
              <header><h1>Event Statistics</h1></header>
              <div class="main">
                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
                <div class="chart-container">
                  <h2>Meteors Detected per Hour (UTC)</h2>
                  <canvas id="hourlyChart"></canvas>
                </div>
                <div class="chart-container">
                  <h2>Meteors Detected per Night</h2>
                  <canvas id="nightlyChart"></canvas>
                </div>
                <p><a href="/{token_param}">← Back to Main Dashboard</a></p>
              </div>
              <script>
                // Use a self-executing async function to fetch data and build charts
                (async () => {{
                    try {{
                        const response = await fetch('/api/event_stats{token_param}');
                        if (!response.ok) {{ throw new Error('Failed to fetch stats'); }}
                        const stats = await response.json();

                        // --- Build Hourly Chart ---
                        const hourlyCtx = document.getElementById('hourlyChart').getContext('2d');
                        const hourlyLabels = Array.from({{length: 24}}, (_, i) => i.toString().padStart(2, '0') + ':00');
                        const hourlyData = hourlyLabels.map((_, i) => stats.by_hour[i.toString()] || 0);

                        new Chart(hourlyCtx, {{
                            type: 'bar',
                            data: {{
                                labels: hourlyLabels,
                                datasets: [{{
                                    label: 'Meteors per Hour',
                                    data: hourlyData,
                                    backgroundColor: 'rgba(97, 175, 239, 0.6)',
                                    borderColor: 'rgba(97, 175, 239, 1)',
                                    borderWidth: 1
                                }}]
                            }},
                            options: {{
                                scales: {{ y: {{ beginAtZero: true, ticks: {{ color: '#e0e0e0' }} }}, x: {{ ticks: {{ color: '#e0e0e0' }} }} }},
                                plugins: {{ legend: {{ labels: {{ color: '#e0e0e0' }} }} }}
                            }}
                        }});

                        // --- Build Nightly Chart ---
                        const nightlyCtx = document.getElementById('nightlyChart').getContext('2d');
                        const sortedNights = Object.keys(stats.by_night).sort();
                        const nightlyLabels = sortedNights;
                        const nightlyData = sortedNights.map(night => stats.by_night[night]);

                        if (nightlyLabels.length > 0) {{
                            new Chart(nightlyCtx, {{
                                type: 'bar',
                                data: {{
                                    labels: nightlyLabels,
                                    datasets: [{{
                                        label: 'Meteors per Night',
                                        data: nightlyData,
                                        backgroundColor: 'rgba(152, 195, 121, 0.6)',
                                        borderColor: 'rgba(152, 195, 121, 1)',
                                        borderWidth: 1
                                    }}]
                                }},
                                options: {{
                                    scales: {{ y: {{ beginAtZero: true, ticks: {{ color: '#e0e0e0' }} }}, x: {{ ticks: {{ color: '#e0e0e0' }} }} }},
                                    plugins: {{ legend: {{ labels: {{ color: '#e0e0e0' }} }} }}
                                }}
                            }});
                        }} else {{
                            nightlyCtx.font = '16px Segoe UI';
                            nightlyCtx.fillStyle = '#999';
                            nightlyCtx.textAlign = 'center';
                            nightlyCtx.fillText('No nightly data available yet.', nightlyCtx.canvas.width / 2, 50);
                        }}

                    }} catch (error) {{
                        console.error('Error loading chart data:', error);
                    }}
                }})();
              </script>
            </body></html>
            """
            return render_template_string(html_page)

        @app.route('/events/<path:filename>')
        @require_token
        def serve_event_file(filename):
            """Serves the captured event images and videos."""
            return send_from_directory(self.events_out_dir, filename)

        @app.route('/output/daylight/<path:filename>')
        @require_token
        def serve_daylight_file(filename):
            """Serves daylight/sky-condition snapshots and the nightly masterpiece."""
            return send_from_directory(self.daylight_out_dir, filename)

        # --- Main Dashboard Page ---
        @app.route('/')
        @require_token
        def dashboard():
#            import html
            token_param = f"?token={DASH_TOKEN}" if DASH_TOKEN else ""

            # --- User Feedback Banners ---
            resume_status = request.args.get('resume_status')
            feedback_html = ""
            if resume_status == 'success':
                feedback_html = '<div class="banner banner-success">Normal operation resumed successfully.</div>'
            elif resume_status == 'ignored':
                feedback_html = '<div class="banner banner-warn">Resume command ignored. The system is currently outside of its scheduled active time. The scheduler will start capture automatically when the time is right.</div>'
            
            # --- Gather Live Metrics ---
            cpu_temp_obj = psutil.sensors_temperatures().get("cpu_thermal", [None])[0]
            cpu_temp_raw = cpu_temp_obj.current if cpu_temp_obj else 0.0
            cpu_temp = f"{cpu_temp_raw:.1f}°C" if cpu_temp_obj else "N/A"
            mem = psutil.virtual_memory()
            mem_used = f"{mem.used / (1024**3):.2f} GB"
            disk = psutil.disk_usage('/')
            disk_used = f"{disk.used / (1024**3):.2f} GB"
            load1, _, _ = os.getloadavg()
            uptime_sec = time.time() - psutil.boot_time()
            uptime_days = int(uptime_sec // 86400)
            uptime_hours = int((uptime_sec % 86400) // 3600)
            uptime_str = f"{uptime_days}d {uptime_hours}h"             
            now = datetime.now()
            system_date = now.strftime('%Y-%m-%d')
            system_time = now.strftime('%H:%M:%S')

            with self.status_lock:
                status = { 
                    "version": self.cfg["general"]["version"],
                    "debug_level": self.cfg["general"].get("debug_level", 0),
                    "capture_active": self.producer_thread_active(), 
                    "is_stable": self.capture_stable.is_set(),
                    "daylight_mode": self.daylight_mode_active.is_set(),
                    "weather_hold": self.weather_hold_active.is_set(),
                    "power_status": self.power_status, 
                    "event_count": self.event_counter, 
                    "last_illuminance": self.last_illuminance, 
                    "last_sky_stddev": self.last_sky_conditions.get("stddev", "N/A"), 
                    "last_sky_stars": self.last_sky_conditions.get("stars", "N/A"), 
                    "last_sky_status": self.last_sky_status,
                    "moon": self.last_moon_status, 
                    "sky_score": self.sky_score,
                    "last_heartbeat": self.last_heartbeat_status, 
                    "live_threads": {t.name for t in (self.control_threads + self.worker_threads) if t.is_alive()},
                    "calib_error": self.last_calibration_error,
                    "last_calib_image": self.last_calibration_image_path,
                    "last_event_files": self.last_event_files.copy(),
                    "is_calibrating": self.is_calibrating.is_set(),
                    "emergency_stop": self.emergency_stop_active.is_set(),
                    "maintenance_mode": self.in_maintenance_mode.is_set(),
                    "is_in_schedule": self.within_schedule(),
                    "current_exposure": self.current_exposure_us,
                    "current_gain": self.current_gain,
                    "effective_threshold": self.last_effective_threshold,
                    "start": self.cfg.get("general", {}).get("start_time"),
                    "end": self.cfg.get("general", {}).get("end_time"),
                    "shutdown": self.cfg.get("general", {}).get("shutdown_time")
                }
                
            if status['is_stable']:
                # Green: Normal Operation
                pipeline_state_str = "Running (Acquiring Data)"
                pipeline_state_class = "ok"
            else:
                # Red: Paused for Maintenance/Tuning
                pipeline_state_str = "ON HOLD (Sky Monitor Check)"
                pipeline_state_class = "err"
                
                # "last_queue_status": self.last_queue_status # Get queue status
#                calib_error = self.last_calibration_error
            with self.maintenance_timeout_lock:
                status["maintenance_timeout_until"] = self.maintenance_timeout_until

            system_status_str = "Active Schedule" if status["is_in_schedule"] else "Idle Schedule"
            system_status_class = "ok" if status["is_in_schedule"] else "warn"

            # 2. Determine the CSS class for illuminance
            illuminance_class = "ok" # Default to green
            try:
                # Convert last_illuminance to a float for comparison
                last_lux_val = float(status['last_illuminance'])
                if last_lux_val > self.cfg.get("daylight", {}).get("lux_threshold"):
                    illuminance_class = "err" # It's daytime, show red
            except (ValueError, TypeError):
                illuminance_class = "warn" # Not a valid number, show yellow

            # 3. Determine the CSS class for star count
            normal_class = "normal"
            bold = "bold"
            stars_class = "ok" # Default to green
            try:
                # Convert last_sky_stars to an int for comparison
                # last_stars_val = int(status['last_sky_stars'])
                if int(status['last_sky_stars']) < self.cfg.get("daylight", {}).get("min_stars"):
                    stars_class = "err" # Not enough stars, show red
            except (ValueError, TypeError):
                stars_class = "warn" # Not a valid number, show yellow
                
            stddev_class = "ok" # Default to green
            try:
                # Convert last_sky_stars to an int for comparison
                # last_stddev_val = int(status['last_sky_stddev'])
                if int(status['last_sky_stddev']) > self.cfg.get("daylight", {}).get("stddev_threshold"):
                    stddev_class = "err" # Not enough stars, show red
            except (ValueError, TypeError):
                stddev_class = "warn" # Not a valid number, show yellow

            threshold = self.cfg["janitor"].get("threshold", 90.0)
            # Get the configured path and use it for the dashboard display.
            monitor_path = self.cfg["janitor"].get("monitor_path", "/")
            disk = psutil.disk_usage(monitor_path)
            disk_used = f"{disk.used / (1024**3):.2f} GB"

            sftp_status_html = ""
            if self.sftp_uploader:
                sftp_class, sftp_message = self.sftp_uploader.get_status()
                sftp_status_html = f'<tr><td>SFTP Status</td><td class="{sftp_class}">{sftp_message}</td></tr>'

            emergency_message_html = ""
            buttons_html = ""
            timeout_message_html = ""            
            config_and_calibration_html = ""
  
            # 4. Read the last 30 lines of the main pipeline log file.
            log_lines = self.read_last_lines(os.path.join(self.general_log_dir, "pipeline.log"), num_lines=30)
            
            # 5. Escape HTML characters for safe rendering and join into a single string.
#            import html
            log_html = "<br>".join(html.escape(line) for line in log_lines)

            # 6. Create the HTML block for the log viewer.
            log_viewer_html = f"""
            <div class="card">
                <h2>Live Log (Last {len(log_lines)} Lines)</h2>
                <div class="log-box">
                    <pre><code>{log_html}</code></pre>
                </div>
            </div>
            """

            # --- Gather Health Statistics ---
            health_stats = self.get_health_statistics(self.health_log_out_file)
            health_stats_html_rows = ""
            if not health_stats:
                health_stats_html_rows = "<tr><td colspan='4'>No health events recorded yet.</td></tr>"
            else:
                for event_data in health_stats:
                    # Use .get() for all keys to prevent KeyErrors if a row is malformed
                    level_class = event_data.get('level', 'INFO').lower()
                    event_type = event_data.get('event_type', 'N/A')
                    count = event_data.get('count', 0)
                    last_message = event_data.get('last_message', '')
                    health_stats_html_rows += f"""
                    <tr>
                        <td class="event-{level_class}">{event_type}</td>
                        <td>{count}</td>
                        <td>{html.escape(last_message)}</td>
                    </tr>
                    """

            health_card_html = f"""
            <div class="card" style="grid-column: 1 / -1;">
                <h2>Health & Event Statistics</h2>
                <div class="log-box" style="height: 200px;">
                    <table style="font-size: 0.8em;">
                        <thead>
                            <tr>
                                <th>Event Type</th>
                                <th>Count</th>
                                <th>Last Message</th>
                            </tr>
                        </thead>
                        <tbody>{health_stats_html_rows}</tbody>
                    </table>
                </div>
            </div>
            """  

            if status["emergency_stop"]:
                emergency_message_html = f"""
                <div style="border: 3px solid #e06c75; background: #3c3c3c; padding: 1.5em; margin: 1em 2em; border-radius: 8px; text-align: center;">
                    <h2 style="color: #e06c75; margin: 0 0 0.5em 0;">EMERGENCY STOP ACTIVATED</h2>
                    <p style="margin: 0; font-size: 1.1em;">
                        A critical error happen, the system is in an unknown state.
                        <br>All new data acquisition has been **HALTED** to prevent system failure.
                    </p>
                    <p style="margin-top: 1em;">
                        <strong>Action Required:</strong> Use the <a href="/files{token_param}">File Manager</a> to manually delete files.
                        <br>A system restart will be required after space has been cleared.
                    </p>
                </div>
                """
                # When in emergency, we explicitly DISABLE all control buttons.
            else:
                if status["maintenance_mode"]:
                    buttons_html = f'<a href="/api/resume{token_param}"><button style="background: #98c379;">Resume Normal Operation</button></a>'

                    # Check if a timeout is currently active
                    with self.maintenance_timeout_lock:
                        if status["maintenance_timeout_until"] > 0:
                            remaining_sec = self.maintenance_timeout_until - time.time()
                            if remaining_sec > 0:
                                remaining_min = int(remaining_sec / 60)
                                timeout_message_html = f"""
                                <div style="border: 2px solid #61afef; padding: 1em; margin-bottom: 1em; border-radius: 8px;">
                                    <p style="color: #61afef; font-weight: bold; margin: 0;">
                                        System is in Maintenance Mode. It will automatically resume in approximately {remaining_min} minute(s).
                                    </p>
                                </div>
                                """
                    # 2. Always generate the settings form HTML.
                    settings_html_rows = ""
                    for section, params in EDITABLE_PARAMS.items():
                        settings_html_rows += f'<tr><th colspan="2">{section.replace("_", " ").title()}</th></tr>'
                        for param in params:
                            value = self.cfg.get(section, {}).get(param, 'N/A')
#                            note = f'<small style="color:#e5c07b;">{interval_note}</small>' if param == "interval_sec" else "" # Simplified for brevity
#                            settings_html_rows += f'<tr><td>{param}</td><td><input type="text" name="{section}_{param}" value="{value}" size="10"> {note}</td></tr>'
                            settings_html_rows += f'<tr><td>{param}</td><td><input type="text" name="{section}_{param}" value="{value}" size="10"></td></tr>'

                    error_box_html = ""
                    if status["calib_error"]:
                        error_box_html = f"""
                        <div class="calibration-box" style="border: 2px solid #e06c75; padding: 1em; background: #3c3c3c;">
                            <h4 style="color: #e06c75; margin-top: 0;">Test Shot Failed</h4>
                            <p style="font-family: monospace; color: #e5c07b;">{status["calib_error"]}</p>
                            <p style="font-size: smaller;">Check `pipeline.log` for more details. You can adjust settings and try again.</p>
                        </div>
                        """
                    if status["is_calibrating"]:
                        config_and_calibration_html = """
                        <div class="config-container">
                            <h2>Maintenance & Calibration</h2>
                            <p style="color: #61afef; font-weight: bold;">TEST SHOT IN PROGRESS... The page will refresh.</p>
                        </div>
                        """
                    else:
                        image_box = ""
                        if self.last_calibration_image_path:
                            image_url = f"/calibration_image{token_param}&t={time.time()}"
                            image_box = f'<h4>Last Test Shot:</h4><a href="{image_url}" target="_blank"><img src="{image_url}" alt="Last Test Shot" width="400"></a>'

                        config_and_calibration_html = f"""
                        <div class="config-container">
                            <form action="/api/save_and_test{token_param}" method="post" id="config-form">
                                <h2>Maintenance & Calibration</h2>
                                <table>{settings_html_rows}</table>
                                <button type="submit">Save Config & Take Test Shot</button>
                            </form>
                            <div class="calibration-box">
                                {error_box_html} 
                                {image_box}
                            </div>
                        </div>
                        """
                else:
                    buttons_html = f'<a href="/api/pause{token_param}"><button style="background: #e5c07b;">Pause Capture & Enter Maintenance Mode</button></a>'
                    timeout_message_html = "" # No timeout message
                    config_and_calibration_html = "" # No config editor

            # --- "LAST EVENT" CARD LOGIC ---
            last_event_html = ""
            last_event = status["last_event_files"]
            event_image_path = last_event.get("image")
            event_video_path = last_event.get("video")

            # Get modification time for cache busting
            evt_mtime = 0
            if event_image_path and os.path.exists(event_image_path):
                evt_mtime = int(os.path.getmtime(event_image_path))

            if event_image_path and os.path.exists(event_image_path):
                # We need a relative path for the URL, not the full system path
                # This assumes 'output' is the static folder for Flask
                relative_image_url = os.path.join('events', os.path.basename(event_image_path))
                video_download_html = ""
                if event_video_path and os.path.exists(event_video_path):
                    relative_video_url = os.path.join('events', os.path.basename(event_video_path))
                    video_download_html = f'<p><a href="/{relative_video_url}{token_param}" download><b>Download Event Video (.mp4)</b></a></p>'

                last_event_html = f"""
                <div class="card" style="grid-column: 1 / -1;">
                    <h2>Last Captured Event</h2>
                    <div style="text-align: center;">
                        <a href="/{relative_image_url}{token_param}" target="_blank">
                            <img src="/{relative_image_url}{token_param}&t={evt_mtime}" alt="Last Event Image" style="max-width: 50%; height: auto; border: 1px solid #555; border-radius: 8px;">
                        </a>
                        <p style="font-size: smaller; color: #999;">{os.path.basename(event_image_path)}</p>
                        {video_download_html}
                    </div>
                </div>
                """
            else:
                last_event_html = f"""
                <div class="card" style="grid-column: 1 / -1;">
                    <h2>Last Captured Event</h2>
                    <p style="text-align: center; color: #999;">Waiting for first event to be detected and processed...</p>
                </div>
                """
            # --- END: "LAST EVENT" CARD LOGIC ---
            
            # Define logic for the weather status string
            if status['weather_hold'] and status['last_sky_status'] == 'ERROR':
                weather_str = "SENSORY ERROR (Safe-Mode Active)"
                weather_class = "err"
            elif status['weather_hold']:
                weather_str = "WEATHER HOLD (Cloudy)"
                weather_class = "warn"
            else:
                weather_str = "SKY CLEAR"
                weather_class = "ok"
                
            # --- DIVINE ANALYTICS CALCULATION --- morning_report
            stats = self.session_stats
            actual_duration = datetime.now(timezone.utc) - stats["start_time"]
            dur_total_min = max(1, int(actual_duration.total_seconds() / 60))
            
            # Use categories for weather %
            measured_min = (stats["clear_sky_minutes"] + stats["cloudy_sky_minutes"]) or 1
            cloud_percent = (stats["cloudy_sky_minutes"] / measured_min) * 100
            
            avg_score = sum(stats["avg_sky_score"]) / len(stats["avg_sky_score"]) if stats["avg_sky_score"] else 0
            
            # Grade Logic
            if avg_score > 1.3: sky_grade = "EXCELLENT (S+)"
            elif avg_score > 1.0: sky_grade = "GOOD (A)"
            elif avg_score > 0.7: sky_grade = "FAIR (B)"
            else: sky_grade = "POOR (C)"

            # Efficiency: How many events vs how much noise
            # A high ratio means the detector is perfectly tuned.
            noise_ratio = stats["rejected_contours"] / (stats["total_events"] or 1)
            
            morning_report_html = f"""
            <div class="card" style="border: 1px solid #c678dd; background: linear-gradient(145deg, #2b2b2b, #352b3b);">
                <h2 style="color: #c678dd;">🌙 Session Mission Report</h2>
                <table style="font-size: 1.1em;">
                    <tr><td>Sky Quality Grade</td><td class="ok" style="color:#c678dd;">{sky_grade}</td></tr>
                    <tr><td>Detection Efficiency</td><td>{stats['total_events']} Events / {stats['rejected_contours']} Noise Blobs</td></tr>
                    <tr><td>Weather Stability</td><td>{100-cloud_percent:.0f}% Clear Sky</td></tr>
                    <tr><td>Moon Influence</td><td>In View for {stats['moon_visible_minutes']} min (Max Impact: {int(stats['max_moon_impact']*100)}%)</td></tr>
                    <tr><td>Session Duration</td><td>{dur_total_min // 60}h {dur_total_min % 60}m</td></tr>
                </table>
                <p style="font-size: 0.8em; color: #888; margin-top: 10px; text-align: center;">
                    "The stars are the streetlights of eternity." - Meteora Intelligence
                </p>
            </div>
            """

            # --- START: NEW PIPELINE MODE LOGIC ---
            pipeline_mode_str = ""
            pipeline_mode_class = ""

            if not status['capture_active']:
                pipeline_mode_str = "Idle (Stopped)"
                pipeline_mode_class = "warn" # Yellow for idle/paused
            elif status['daylight_mode'] or status['weather_hold']:
                reason = "Daylight" if status['daylight_mode'] else "Weather Hold"
                pipeline_mode_str = f"Timelapse Only ({reason})"
                pipeline_mode_class = "warn" # Yellow for partial operation
            else:
                pipeline_mode_str = "Timelapse + Events"
                pipeline_mode_class = "ok" # Green for full operation
            # --- END: NEW PIPELINE MODE LOGIC ---

            # Get the narrative
            narrative = self.get_cosmic_narrative(status['sky_score'], status['last_sky_stars'], status['moon']['alt'] > 0)
            # Get modification time for cache busting
            nrt_mtime = 0
            if self.daylight_out_dir and os.path.exists(self.daylight_out_dir):
                nrt_mtime = int(os.path.getmtime(self.daylight_out_dir))
            
            token_qs = f"&token={DASH_TOKEN}" if DASH_TOKEN else ""
            # HTML Row
            narrative_html = f"""
            <div class="card" style="grid-column: 1 / -1; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border: 1px solid #4ecca3;">
                <h2 style="color: #4ecca3; font-style: italic;">"{narrative}"</h2>
                <p style="color: #999;">The system has tracked {status['event_count']} cosmic paths tonight.</p>
                <img src="/output/daylight/nightly_masterpiece_live.png?t={nrt_mtime}{token_qs}" style="max-width: 50%; border: 1px solid #4ecca3; border-radius: 4px;">
                <p style="font-size: 0.8em; color: #4ecca3;">&uarr; This image is the accumulated history of every meteor caught since sunset.</p>
            </div>
            """

            debug_level_row = ""
            if status['debug_level'] > 0:
                # We use the 'warn' class (yellow) to indicate that debug overhead is active
                debug_level_row = f'<tr><td>Debug Mode</td><td class="warn">Level {status["debug_level"]} Active</td></tr>'
            
            # --- GENERATE ALL HTML COMPONENTS ---
            pipeline_status_rows = f"""
                {debug_level_row}
                <tr><td>System Status</td><td class="{system_status_class}">{system_status_str}</td></tr>
                <tr><td>Capture Active</td><td class="{'ok' if status['capture_active'] else 'warn'}">{status['capture_active']}</td></tr>
                <tr><td>Pipeline State</td><td class="{pipeline_state_class}">{pipeline_state_str}</td></tr>
                <tr><td>Pipeline Mode</td><td class="{pipeline_mode_class}">{pipeline_mode_str}</td></tr>
                <tr><td>Weather Condition</td><td class="{weather_class}">{weather_str}</td></tr>
                <tr><td>Moon Altitude</td><td>{status['moon']['alt']}°</td></tr>
                <tr><td>Moon View</td><td class="{'err' if status['moon']['in_fov'] else 'ok'}">{'IN FOV' if status['moon']['in_fov'] else 'Clear'}</td></tr>
                <tr><td>Moon Interference</td><td>{int(status['moon']['impact'] * 100)}%</td></tr>
                <tr><td>Effective Threshold</td><td>{status['effective_threshold']}</td></tr>
                <tr><td>Power Status</td><td class="{'ok' if status['power_status'] == 'OK' else 'warn'}">{status['power_status']}</td></tr>
                <tr><td>Events Captured</td><td>{status['event_count']}</td></tr>
                <tr><td>Last Illuminance (Lux)</td><td class="{illuminance_class}">{status['last_illuminance']}</td></tr>
                <tr>
                    <td>Last Sky StdDev</td>
                    <td class="{bold}">
                        <span class="{stddev_class}">{status['last_sky_stddev'] if status['last_sky_stddev'] == 'N/A' else f'{float(status['last_sky_stddev']):.1f}'}</span> /
                        <span class="{normal_class}">{self.cfg["daylight"]["stddev_threshold"]}</span>
                    </td>
                </tr>
                <tr>
                    <td>Stars</td>
                    <td class="{bold}">
                        <span class="{stars_class}">{status['last_sky_stars']}</span> /
                        <span class="{normal_class}">{self.cfg["daylight"]["min_stars"]}</span>
                    </td>
                </tr>
                <tr><td>Last Heartbeat</td><td>{status['last_heartbeat']}</td></tr>
                <tr><td>Exposure (us)</td><td>{status['current_exposure']}</td></tr>
                <tr><td>Gain</td><td>{status['current_gain']:.2f}</td></tr>
                {sftp_status_html}
                <tr><td>Start Acquisition</td><td>{status["start"]}</td></tr>
                <tr><td>End Acquisition</td><td>{status["end"]}</td></tr>
                <tr><td>Shutdown System</td><td>{status["shutdown"]}</td></tr>
            """
                # {queue_status_html}
            # """
            
            # Component: Threads Table
            all_thread_names = sorted(list(set([t.name for t in self.control_threads] + [t.name for t in self.worker_threads])))
            threads_html_rows = ""
            for name in all_thread_names:
                 status_html = '<td class="ok">Running</td>' if name in status["live_threads"] else '<td class="err">Stopped</td>'
                 threads_html_rows += f"<tr><td>{name}</td>{status_html}</tr>"
            
            # Component: System Vitals Table
            system_vitals_rows = f"""
                <tr><td>System Date</td><td>{system_date}</td></tr>
                <tr><td>System Time (Local)</td><td>{system_time}</td></tr>            
                <tr><td>CPU Temperature</td><td class="{'warn' if cpu_temp_raw > 75 else 'ok'}">{cpu_temp}</td></tr>
                <tr><td>Memory Usage</td><td class="{'warn' if mem.percent > 80 else 'ok'}">{mem_used} ({mem.percent}%)</td></tr>
                <tr><td>Disk Usage ({monitor_path})</td><td class="{'err' if disk.percent > 90 else 'warn' if disk.percent > threshold else 'ok'}">{disk_used} ({disk.percent}%)</td></tr>
                <tr><td>Load Average (1m)</td><td>{load1:.2f}</td></tr>
                <tr><td>Uptime</td><td>{uptime_str}</td></tr>
            """            
           
            picture_box_html = ""
            # is_timelapse_enabled = self.cfg["timelapse"].get("stack_N", 0) > 0
            
            # Show the picture box only if timelapse is enabled AND the pipeline is active
            if self.cfg["timelapse"].get("stack_N", 0) > 0 and status["capture_active"] and not status["maintenance_mode"]:
                # Find the latest file here to get its timestamp
                latest_tl_path = self.get_latest_file(TIMELAPSE_DIR)
                tl_mtime = 0
                if latest_tl_path and os.path.exists(latest_tl_path):
                    tl_mtime = int(os.path.getmtime(latest_tl_path))                
                
                # The 't={time.time()}' part is a cache-buster to ensure the browser always fetches the latest image
                token_qs = f"&token={DASH_TOKEN}" if DASH_TOKEN else ""
                image_url = f"/latest_timelapse_image?t={tl_mtime}{token_qs}"
                # image_url = f"/latest_timelapse_image{token_param}&t={tl_mtime}"
                picture_box_html = f"""
                <div class="picture-box-container">
                    <h2>Latest Timelapse</h2>
                    <a href="{image_url}" target="_blank">
                        <img src="{image_url}" alt="Latest Timelapse Image" style="max-width: 50%; height: auto; border: 1px solid #555;">
                    </a>
                    <p style="font-size: smaller; color: #999;">This image updates automatically. Click to view full size.</p>
                </div>
                """

            # --- FOOTER TABLE ---
            footer_table_html = f"""
            
            <div class="section" style="padding: 0 2em;">
            <div class="card">
            <div class="log-box">
                <table class="footer-nav">
                    <thead>
                        <tr>
                            <th>Logs</th>
                            <th>File Management</th>
                            <th>Configuration</th>
                            <th>Data Analysis</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td><a href="/logs/pipeline.log{token_param}">pipeline.log</a></td>
                            <td><a href="/files{token_param}">Browse Event & Timelapse Files</a></td>
                            <td><a href="/settings{token_param}">Edit Full Configuration</a></td>
                            <td><a href="/stats{token_param}">View Event Statistics</a></td>
                        </tr>
                        <tr>
                            <td><a href="/logs/system_monitor.csv{token_param}">system_monitor.csv</a></td>
                            <td></td>
                            <td><a href="/system_time{token_param}">Set System Time</a></td>
                            <td></td>
                        </tr>
                        <tr>
                            <td><a href="/logs/events.csv{token_param}">events.csv</a></td>
                            <td></td>
                            <td></td>
                            <td></td>
                        </tr>
                        <tr>
                            <td><a href="/logs/health_stats.csv{token_param}">health_stats.csv</a></td>
                            <td></td>
                            <td></td>
                            <td></td>
                        </tr>
                    </tbody>
                </table>
            </div>
            </div>
            </div>
            """

            refresh_url = f"/{token_param}"
            html_page = f"""
            <!DOCTYPE html><html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>Meteora Pipeline Dashboard</title>
              <meta http-equiv="refresh" content="10; url={refresh_url}">
              <style>
                body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #1e1e1e; color: #e0e0e0; margin: 0; padding: 0; }}
                header {{ background: #2c2c2c; padding: 1em 2em; border-bottom: 1px solid #444; }}
                h1 {{ margin: 0; color: #61afef; }}
                .main {{ padding: 2em; display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
                .card {{ background: #2b2b2b; border-radius: 12px; padding: 1em; box-shadow: 0 2px 6px rgba(0,0,0,0.5); }}
                .card h2 {{ margin-top: 0; font-size: 1.2em; color: #98c379; }}
                table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
                th, td {{ padding: 6px 8px; border-bottom: 1px solid #444; }}
                th {{ text-align: left; color: #61afef; }}
                .ok {{ color: #98c379; font-weight: bold; }}
                .warn {{ color: #e5c07b; font-weight: bold; }}
                .err {{ color: #e06c75; font-weight: bold; }}
                .normal {{ color: #61AFEF; font-weight: bold; }} 
                .bold {{ font-weight: bold; }} 
                button {{ background: #61afef; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; font-size: 0.9em; margin: 5px 0; transition: 0.2s; }}
                button:hover {{ background: #4fa3d8; }}
                .btn-danger {{ background: #e06c75; }}
                .btn-danger:hover {{ background: #c65c66; }}
                .banner {{ padding: 1em; margin: 0 2em 1em 2em; border-radius: 8px; text-align: center; font-weight: bold; }}
                .banner-success {{ background-color: #2c5f2d; color: #97f097; }}
                .banner-warn {{ background-color: #5c5c00; color: #e5c07b; }}
                img {{ max-width: 100%; border-radius: 6px; margin-top: 10px; }}
                .section {{ margin-top: 2em; }}
                .log-box {{ background-color: #1a1a1a; border: 1px solid #444; border-radius: 6px; padding: 10px; height: 300px; overflow-y: scroll; font-size: 0.8em; }}
                .log-box pre, .log-box code {{ margin: 0; padding: 0; white-space: pre-wrap; word-wrap: break-word; }}    
                .event-warning {{ color: #e5c07b; font-weight: bold; }}
                .event-error {{ color: #e06c75; font-weight: bold; }}
                .event-critical {{ color: #e06c75; font-weight: bold; text-transform: uppercase; }}  
                .footer-nav th {{ color: #98c379; font-size: 1.3em; border-bottom: 2px solid #444; padding-bottom: 10px; }}
                .footer-nav td {{ padding-top: 10px; padding-bottom: 10px; border-bottom: none; }}
                .footer-nav a {{ font-size: 1.2em; }}
              </style>
            </head>
            <body>
              <header>
                <h1>Meteora Pipeline Status (v{status['version']})</h1>
              </header>
              {feedback_html}
              {emergency_message_html}
              <div class="main">
                <div class="card">
                  <h2>Pipeline Status</h2>
                  <table>{pipeline_status_rows}</table>
                </div>
                <div class="card">
                  <h2>Thread Status</h2>
                  <table>{threads_html_rows}</table>
                </div>
                <div class="card">
                  <h2>System Vitals</h2>
                  <table>{system_vitals_rows}</table>
                </div>                               
              </div>
              <div class="section" style="padding: 0 2em;">
                {last_event_html}
              </div>
              <div class="section" style="padding: 0 2em;">
              {narrative_html}
              </div>
              <div class="section" style="padding: 0 2em;">
                {health_card_html}
              </div>
              <div class="section" style="padding: 0 2em;">
                {morning_report_html}
              </div>
              <div class="section" style="padding: 0 2em;">
                {log_viewer_html}
              </div>              
              <div class="section" style="padding: 0 2em;">
                {buttons_html}
                {timeout_message_html}
                {picture_box_html}
                {config_and_calibration_html}
              </div>
              <div class="section" style="padding: 0 2em;>
                {footer_table_html}
              </div>
            </body>
            </html>
            """
            
            # -------- Save the generated HTML to a file for troubleshooting
            # try:
                # debug_path = os.path.join(self.general_log_dir, "dashboard_debug.html")
                # with open(debug_path, "w") as f:
                    # f.write(html_file)
            # except Exception as e:
                # # Log this error but don't crash the dashboard
                # logging.warning("Could not save dashboard debug HTML file: %s", e)            
            # -------- 
            
            return render_template_string(html_page)        

        # --- Log Download Endpoint (unchanged) ---
        @app.route('/logs/<path:filename>')
        @require_token
        def download_log(filename):
            log_dir = os.path.abspath(self.general_log_dir)
            allowed_files = ["pipeline.log", "system_monitor.csv", "events.csv", "health_stats.csv"]
            if filename in allowed_files:
                return send_from_directory(log_dir, filename, as_attachment=True)
            else:
                return "File not found.", 404

        host = dash_cfg.get("host", "0.0.0.0")
        port = dash_cfg.get("port", 5000)
        logging.info("Starting status dashboard. Access at http://%s:%d or http://<IP_ADDRESS>:%d", socket.gethostname(), port, port)
        app.logger.disabled = True
        logging.getLogger('werkzeug').disabled = True
        
        try:
            app.run(host=host, port=port)
        except OSError as e:
            logging.error("Could not start dashboard on port %d: %s. Is another process using it?", port, e)
    # -----------------
    def led_status_loop(self):
        """
        A dedicated thread that handles the startup signal AND the status indication.
        Consolidating logic here prevents race conditions on the GPIO line.
        """
        if not self.led_line:
            return

        logging.info("LED Status thread started. Waiting for System Ready...")

        # 1. Wait for the main thread to signal that initialization is done
        if not self.system_ready.wait(timeout=30):
            logging.warning("LED: Timed out waiting for System Ready signal.")

        # 2. Perform Startup Signal (Triple Blink)
        # This is safe here because this thread has exclusive control of the line
        try:
            self.led_line.set_value(self.led_pin, gpiod.line.Value.INACTIVE)
            time.sleep(0.5)
            for _ in range(3):
                self.led_line.set_value(self.led_pin, gpiod.line.Value.ACTIVE)
                time.sleep(0.15)
                self.led_line.set_value(self.led_pin, gpiod.line.Value.INACTIVE)
                time.sleep(0.15)
            logging.info("LED startup signal complete.")
        except Exception as e:
            logging.error(f"LED startup signal failed: {e}")

        logging.info("LED entering continuous status loop.")
        
        # 3. Enter the main status indication loop
        while self.running.is_set():
            # --- Default: A slow "breathing" pulse for idle ---
            on_time, off_time = 0.1, 2.5 

            try:
                # Determine pattern based on system state priority
                if self.emergency_stop_active.is_set() or self.camera_fatal_error.is_set():
                    on_time, off_time = 0.1, 0.1 # Rapid flicker for ERROR

                elif self.event_in_progress.is_set():
                    on_time, off_time = 0.25, 0.25 # Fast blink for EVENT

                elif self.producer_thread_active():
                    on_time, off_time = 1.0, 1.0 # Slow pulse for CAPTURING
                
                # Execute the pattern
                self.led_line.set_value(self.led_pin, gpiod.line.Value.ACTIVE)
                
                # Check running flag frequently during sleep to allow fast shutdown
                expiry = time.time() + on_time
                while self.running.is_set() and time.time() < expiry:
                    time.sleep(0.1)
                    
                if not self.running.is_set():
                    break
                
                self.led_line.set_value(self.led_pin, gpiod.line.Value.INACTIVE)
                expiry = time.time() + off_time
                while self.running.is_set() and time.time() < expiry:
                    time.sleep(0.1)

            except Exception as e:
                logging.error(f"Error in LED status loop: {e}")
                time.sleep(5)
    # -----------------
    def publish_debug(self, msg_type, payload, image=None):
        """
        Standardized ZMQ publisher.
        msg_type: 'stats', 'event', 'alignment'
        payload: dict
        image: optional numpy array
        """
        # Level 0 = Off
        if self.cfg["general"]["debug_level"] < 1: return
        
        # Level 1 = Stats only (no images over ZMQ unless level 3)
        if image is not None and self.cfg["general"]["debug_level"] < 3:
            image = None

        try:
            # Add common metadata
            payload['type'] = msg_type
            payload['ts_utc'] = datetime.now(timezone.utc).isoformat()
            
            # Serialize JSON
            json_header = json.dumps(payload, default=str).encode('utf-8')
            
            if image is not None:
                ret, jpg = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 70])
                if ret:
                    # Send multipart: [Topic, JSON, ImageBytes]
                    self.debug_pub.send_multipart([b"debug", json_header, jpg.tobytes()], flags=zmq.NOBLOCK)
            else:
                self.debug_pub.send_multipart([b"debug", json_header], flags=zmq.NOBLOCK)
        except Exception as e:
            # Never crash the pipeline due to a debug error
            logging.error(f"Debug Publish Error: {e}")
    # -----------------
    def publish_state_telemetry(self):
        """Sends the full internal variable state via ZMQ."""
        # Only run if debug is enabled to save CPU
        if self.cfg["general"]["debug_level"] < 1:
            return

        try:
            data = self.get_telemetry_data()
            payload = {
                "type": "state_telemetry",
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "variables": data
            }
            # Serialize and send (No image part)
            json_header = json.dumps(payload, default=str).encode('utf-8')
            self.debug_pub.send_multipart([b"debug", json_header], flags=zmq.NOBLOCK)
        except Exception as e:
            logging.error(f"Telemetry Publish Error: {e}")
    # -----------------
    def get_telemetry_data(self):
        """Filters self.__dict__ for logic variables, excluding all images and hardware objects."""
        telemetry = {}
        
        # 1. Define objects that should be ignored or summarized
        # We explicitly exclude frames/arrays to keep the packet small
        exclude_types = (np.ndarray, cv2.VideoCapture, zmq.Socket, threading.Lock, Picamera2)
        exclude_keys = ['background', 'master_dark', 'flat_multiplier', 'camera', 'debug_pub']

        for key, value in self.__dict__.items():
            if key in exclude_keys or isinstance(value, exclude_types):
                continue

            # --- Convert Complex Objects to Serializables ---
            
            # Thread Status
            if isinstance(value, threading.Thread):
                telemetry[key] = f"Thread(Alive={value.is_alive()})"
            
            # Event Status (Booleans)
            elif isinstance(value, threading.Event):
                telemetry[key] = value.is_set()
            
            # Queue Status (Current Depth)
            elif isinstance(value, queue.Queue):
                telemetry[key] = f"Queue(Size={value.qsize()})"
            
            # Enums (SkyConditionStatus)
            elif isinstance(value, Enum):
                telemetry[key] = value.name
            
            # Standard Primitives
            elif isinstance(value, (int, float, str, bool, type(None), list, dict)):
                telemetry[key] = value
            
            # Everything else to string
            else:
                telemetry[key] = str(value)
                
        return telemetry
    # -----------------
    # 10. UTILITIES & STATIC METHODS
    # -----------------
    def _enqueue_timelapse_frame(self, ts, frame, meta_label):
        """
        Helper: Enqueues frames for the timelapse worker.
        Takes a full stack of N frames back-to-back (burst), then pauses for the configured interval.
        Includes an emergency frame-throttle to prevent RAM crashes if the queue overflows.
        """
        # 1. Enforce the timer (This handles both the Stack Interval and the Emergency Throttle)
        if time.time() < self.timelapse_next_capture_time:
            return

        try:
            # 2. Attempt to queue the frame
            self.timelapse_q.put_nowait((ts, frame, meta_label))
            self.timelapse_frames_queued += 1

            # 3. Check if we just completed a full batch/stack
            if self.timelapse_frames_queued >= self.cfg["timelapse"]["stack_N"]:
                # BATCH COMPLETE: Now we wait the full interval before starting the next batch
                self.timelapse_next_capture_time = time.time() + self.timelapse_interval_sec
                self.timelapse_frames_queued = 0
                
                # Reset the emergency throttle because the long pause will naturally drain the queue
                self.emergency_frame_throttle = 0.0 
            else:
                # STILL BURSTING: Queue the next frame instantly (subject only to the emergency throttle)
                self.timelapse_next_capture_time = time.time() + self.emergency_frame_throttle

                # Auto-Recovery: If we were previously throttling, but the queue is now healthy (<50%), speed back up
                if self.emergency_frame_throttle > 0 and self.timelapse_q.qsize() < (self.effective_max_q * 0.5):
                    self.emergency_frame_throttle = max(0.0, self.emergency_frame_throttle - 0.1)
                    
                # self.log_health_event("CRITICAL", "TIMELAPSE_QUEUE_FULL", "INCREASE INTERVAL TIME.")

        except queue.Full:
            # 4. EMERGENCY RAM PROTECTION (Queue is full!)
            # Hardware is producing frames much faster than CPU can stack them.
            # Force a hard brake (+0.5s) between *individual* frames to let the stacker catch up.
            self.emergency_frame_throttle += 0.5
            self.timelapse_next_capture_time = time.time() + self.emergency_frame_throttle
            
            logging.debug(f"Timelapse queue full! Emergency inter-frame throttle applied: {self.emergency_frame_throttle:.1f}s.")
            self.log_health_event("WARNING", "TIMELAPSE_THROTTLE", f"Emergency throttle: {self.emergency_frame_throttle:.1f}s")
    # -----------------
    def _register_camera_failure(self):
        """
        Increments the consecutive camera failure counter globally.
        Returns False if the maximum failure limit is reached (fatal), True if safe to retry.
        """
        self.consecutive_camera_failures += 1
        max_failures = self.cfg["general"].get("max_camera_failures", 20)
        logging.warning("Camera read failed (%d/%d consecutive failures).", self.consecutive_camera_failures, max_failures)
        
        if self.consecutive_camera_failures >= max_failures:
            logging.critical("Camera has failed %d consecutive times. Setting fatal error flag.", max_failures)
            self.camera_fatal_error.set()
            return False
            
        return True
    # -----------------
    def _reset_camera_failure(self):
        """Resets the consecutive camera failure counter upon a successful frame read."""
        if self.consecutive_camera_failures > 0:
            self.consecutive_camera_failures = 0    
    # -----------------   
    def within_schedule(self):
        now = datetime.now()
        start_h, start_m = map(int, self.cfg["general"]["start_time"].split(":"))
        end_h, end_m = map(int, self.cfg["general"]["end_time"].split(":"))
        start = dtime(start_h, start_m)
        end = dtime(end_h, end_m)
        current = now.time()
        if start <= end:
            return start <= current <= end
        else:
            return current >= start or current <= end
    # -----------------
    def read_last_lines(self, filepath, num_lines=50):
        """
        Efficiently reads the last N lines from a file.
        Returns a list of strings, or an empty list if the file doesn't exist.
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                # Use a deque for efficient appending to the front
                lines = deque(maxlen=num_lines)
                for line in f:
                    lines.append(line.strip())
                return list(lines)
        except FileNotFoundError:
            return ["Log file not found."]
        except Exception as e:
            return [f"Error reading log file: {e}"]
    # -----------------
    def get_file_list(self, path):
        """Scans a directory and returns a list of file details."""
        files = []
        if not os.path.isdir(path):
            return files
            
        for entry in os.scandir(path):
            # Skip directories and hidden files
            if entry.is_file() and not entry.name.startswith('.'):
                try:
                    stat = entry.stat()
                    files.append({
                        'name': entry.name,
                        'path': entry.path,
                        'size_mb': f"{stat.st_size / (1024*1024):.2f} MB",
                        'mtime': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    })
                except FileNotFoundError:
                    continue # File might have been deleted between scan and stat
        
        # Sort files by modification time, newest first
        files.sort(key=lambda x: x['mtime'], reverse=True)
        return files
    # -----------------
    def is_safe_path(self, base_dirs, path_to_check):
        """
        CRITICAL SECURITY FUNCTION: Checks if a file path is safely within one of
        the allowed base directories to prevent path traversal attacks.
        """
        try:
            # Resolve the absolute path to prevent '..' tricks
            resolved_path = os.path.abspath(path_to_check)
            
            # Check if the resolved path starts with any of the allowed base directories
            for base in base_dirs:
                # if resolved_path.startswith(os.path.abspath(base)):
                if resolved_path.startswith(os.path.abspath(base) + os.sep):
                    return True
        except Exception:
            return False
        return False
    # -----------------
    def get_latest_file(self, path, extension=".png"):
        """Finds the most recently modified file with a given extension in a directory."""
        files = []
        if not os.path.isdir(path):
            return None
            
        for entry in os.scandir(path):
            if entry.is_file() and entry.name.lower().endswith(extension):
                try:
                    files.append(entry.path)
                except FileNotFoundError:
                    continue
        
        if not files:
            return None
            
        # Return the file with the latest modification time
        return max(files, key=os.path.getctime)
    # -----------------
    def get_core_voltage(self):
        """
        Gets the core voltage for the current Raspberry Pi model, attempting multiple methods for robustness.
        Supports Raspberry Pi 5 (sysfs) and older models (vcgencmd).
        Returns the voltage in Volts, or 0.0 on error.
        """
        # --- Method 1: Try the modern sysfs interface first (for Pi 5) ---
        try:
            # Find the hwmon device for the DA9091 PMIC
            for path in glob.glob('/sys/class/hwmon/hwmon*/name'):
                with open(path, 'r') as f:
                    if f.read().strip() == 'da9091':
                        hwmon_path = os.path.dirname(path)
                        # Find the specific voltage input file labeled "VDD_CORE"
                        for label_path in glob.glob(os.path.join(hwmon_path, 'in*_label')):
                            with open(label_path, 'r') as f_label:
                                if f_label.read().strip() == 'VDD_CORE':
                                    input_path = label_path.replace('_label', '_input')
                                    with open(input_path, 'r') as f_in:
                                        # Read value (in millivolts) and convert to Volts
                                        return int(f_in.read().strip()) / 1000.0
        except (IOError, ValueError, FileNotFoundError):
            # This will catch permission errors or cases where files don't exist.
            # We don't log an error here, just silently fall back to the next method.
            pass

        # --- Method 2: Fallback to vcgencmd (for Pi 4 and as a backup for Pi 5) ---
        try:
            # result = subprocess.run(['vcgencmd', 'measure_volts', 'core'], capture_output=True, text=True, check=True)            
            result = subprocess.run(['vcgencmd', 'measure_volts', 'core'],capture_output=True, text=True, check=True, timeout=3)
            
            # Output is typically "volt=1.2345V"
            voltage_str = result.stdout.split('=')[1].replace('V', '').strip()
            return float(voltage_str)
        except (subprocess.CalledProcessError, FileNotFoundError, IndexError, ValueError, subprocess.TimeoutExpired):
            # This will catch errors if vcgencmd isn't found, fails, or returns unexpected output.
            pass

        # --- Final Fallback ---
        # If both methods fail, return 0.0
        return 0.0
    # -----------------
    def ensure_dir(self, path):
        """Ensure that a directory exists."""
        os.makedirs(path, exist_ok=True)
    # -----------------
    @classmethod
    def create_master_dark(cls, config_path, num_frames=50, out_path="master_dark.png"):
        """
        Captures a series of dark frames with the lens capped and averages them
        to create a master dark frame for noise reduction.
        """
        
        if not os.path.exists(config_path):
            print(f"ERROR: Configuration file not found at '{config_path}'. Cannot create dark frame.")
            sys.exit(1)
            
        print(f"Loading capture settings from '{config_path}'...")
        cfg = cls.load_and_validate_config(config_path)
        capture_cfg = cfg.get("capture", {})

        width = capture_cfg.get("width")
        height = capture_cfg.get("height")
        exposure_us = capture_cfg.get("exposure_us")
        gain = capture_cfg.get("gain")

        if not all([width, height, exposure_us, gain]):
            print("Error: One or more required capture settings (width, height, exposure_us, gain) are missing from the config.")
            return

        print("--- Master Dark Frame Creation Utility ---")
        print("\nSettings loaded from config file:")
        print(f"  Resolution:   {width}x{height}")
        print(f"  Exposure:     {exposure_us} us")
        print(f"  Analogue Gain: {gain}")
        print("\nIMPORTANT: Ensure the camera lens is completely covered and light-proof.")
        input("Press Enter to begin capturing dark frames...")

        try:
            # 1. Initialize Picamera2 with the exact same settings as the main pipeline
            picam2 = Picamera2()
            # config = picam2.create_still_configuration(main={"size": (width, height)})
            # picam2.configure(config)
            main_config = picam2.create_still_configuration(main={"format": "RGB888", "size": (width, height)})
            picam2.configure(main_config)
            controls = {"ExposureTime": exposure_us, "AnalogueGain": gain, "AwbEnable": False}
            picam2.set_controls(controls)
            picam2.start()
            time.sleep(2)

            print(f"\nCapturing {num_frames} dark frames...")
            accumulator = np.zeros((height, width, 3), dtype=np.float64)

            # 2. Capture and accumulate frames
            for i in range(num_frames):
                # --- FIX: Mirror the main pipeline's raw unpacking logic ---
                req = picam2.capture_request()
                try:
                    img_array = req.make_array("main")
                finally:
                    req.release()

                if img_array is None:
                    print(f"Error: Failed to capture frame {i+1}. Aborting.")
                    if i == 0: 
                        return # Don't process a completely empty stack
                    break
                
                # Convert the Bayer pattern to 16-bit BGR
                bgr_8bit = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
                frame = (bgr_8bit.astype(np.uint16) * 256)
                
                accumulator += frame.astype(np.float64)
                print(f"  Captured frame {i+1}/{num_frames}")

            picam2.stop()
            picam2.close()
            print("\nCapture complete.")

            # 3. Calculate the average and convert to a savable format
            print("Averaging frames to create master dark...")
            master_dark_float = accumulator / num_frames
            master_dark = master_dark_float.clip(0, 65535).astype(np.uint16)

            # 4. Save the final image
            cv2.imwrite(out_path, master_dark)
            print(f"\nSUCCESS: Master dark frame saved to '{out_path}'")
            print("You can now add this path to your main config file under the 'detection' section.")

        except Exception as e:
            print(f"\nAn error occurred: {e}")
            print("Please ensure picamera2 is installed and the camera is connected correctly.")
            if 'picam2' in locals() and picam2.started:
                picam2.stop()
                picam2.close()                                                    
    # -----------------
    @classmethod
    def load_and_validate_config(cls, path=None, merge=True):
        """
        Loads a config from a file path, checks for typos/unknown keys,
        merges with defaults (unless merge=False), and validates using Pydantic.
        """
        # If the caller only wants the raw, unmerged file, use the legacy loader.
        if not merge:
            return cls.legacy_load_config(path, merge=False)

        user_cfg = {}
        if path and os.path.exists(path):
            try:
                with open(path, "r") as f:
                    user_cfg = json.load(f)
            except json.JSONDecodeError as e:
                print(f"FATAL: Error decoding JSON from config file '{path}': {e}", file=sys.stderr)
                sys.exit(1)

            # --- NEW: Check for Typos / Unknown Keys ---
            unknown_keys = cls._validate_keys_exist(user_cfg, DEFAULTS)
            if unknown_keys:
                print("="*60, file=sys.stderr)
                print("FATAL: CONFIGURATION ERROR - UNKNOWN KEYS FOUND", file=sys.stderr)
                print("The following keys in your config file are not recognized:", file=sys.stderr)
                for k in unknown_keys:
                    print(f"  - {k}", file=sys.stderr)
                print("\nPlease check for spelling errors or obsolete settings.", file=sys.stderr)
                print("="*60, file=sys.stderr)
                sys.exit(1)
            # -------------------------------------------
        
        try:
            # Pydantic's model_validate will automatically use the defaults defined
            # in the schema for any keys missing from user_cfg. This is the merge step.
            validated_model = MainConfig.model_validate(user_cfg)
            validated_dict = validated_model.model_dump()
            
            # Only print this message on initial startup
            if path:
                print("Configuration loaded, structure verified, and validated successfully.")
            return validated_dict
        except ValidationError  as e: # Catches Pydantic's ValidationError
            print("="*60, file=sys.stderr)
            print("FATAL: CONFIGURATION VALIDATION FAILED", file=sys.stderr)
            print("="*60, file=sys.stderr)
            print(f"Error details:\n{e}", file=sys.stderr)
            print("\nPlease correct your config file and try again.", file=sys.stderr)
            sys.exit(1)
    # -----------------
    @classmethod
    def _validate_keys_exist(cls, user_cfg, default_cfg, parent_path=""):
        """
        Recursively checks if keys in user_cfg exist in default_cfg.
        Returns a list of unknown key paths.
        """
        unknown_keys = []
        for key, value in user_cfg.items():
            current_path = f"{parent_path}.{key}" if parent_path else key
            
            if key not in default_cfg:
                unknown_keys.append(current_path)
            elif isinstance(value, dict) and isinstance(default_cfg.get(key), dict):
                # Recurse into nested dictionaries
                unknown_keys.extend(cls._validate_keys_exist(value, default_cfg[key], current_path))
        
        return unknown_keys
    # -----------------
    @classmethod
    def legacy_load_config(cls, path=None, merge=True):
        # This is the old load_config, kept as a fallback.
        import copy
        if merge:
            cfg = json.loads(json.dumps(DEFAULTS))    # copy.deepcopy(DEFAULTS)
        else:
            cfg = {}
        if path and os.path.exists(path):
            with open(path, "r") as f:
                user_cfg = json.load(f)
            if merge:
                for k, v in user_cfg.items():
                    if isinstance(v, dict) and k in cfg:
                        cfg[k].update(v)
                    else:
                        cfg[k] = v
            else:
                cfg = user_cfg.copy()
        return cfg
    # -----------------
    @classmethod 
    def save_json_atomic(cls, path, data):
        try:
            temp_path = path + ".tmp"
            with open(temp_path, 'w') as f:
                json.dump(data, f, indent=4)
            os.rename(temp_path, path)
        except Exception as e:
            logging.error("Failed to save JSON atomically to %s: %s", path, e)
    # -----------------
# -------------------------------------------------------------------------------
# MAIN EXECUTION BLOCK
# -------------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Meteora Pipeline Pi5")
    parser.add_argument("--config", type=str, help="Path to config JSON file", default="config.json")
    parser.add_argument("--create_config", action="store_true", help="Create a default config.json file and exit.")
    parser.add_argument("--dark_frame", action="store_true", help="Create the dark frame and exit.")
    parser.add_argument("--bias_frame", action="store_true", help="Capture 50 frames at min exposure for Bias.")
    parser.add_argument("--flat_frame", action="store_true", help="Capture 50 frames (auto-exposed) for Flat.")
    parser.add_argument("--simulate", action="store_true", help="Run in simulation mode with a fake camera feed.")
    args = parser.parse_args()

    # Helper to capture calibration stacks
    def capture_calib_stack(pipeline, count, desc, out_name):
        print(f"Capturing {count} {desc} frames...")
        # If flat, we might want Auto Exposure on to get a grey image
        # If bias, we want min exposure
        accumulator = None
        frames_captured = 0
        for i in range(count):
            frame = pipeline.camera.read()
            if frame is None: 
                print(f"  WARNING: Frame {i+1} failed", file=sys.stderr)
                continue
            if accumulator is None:
                accumulator = frame.astype(np.float64)
            else:
                accumulator += frame.astype(np.float64)
            
            frames_captured += 1
            print(f"  Frame {i+1}/{count}", end='\r')
        
        if accumulator is not None:
            print (f" Total frames: {count} - Acquired:{frames_captured}")
            # avg = (accumulator / count).clip(0, 255).astype(np.uint8)
            avg = (accumulator / frames_captured).clip(0, 65535).astype(np.uint16)
            cv2.imwrite(out_name, avg)
            print(f"\nSaved {desc} to {out_name}")
        else:
            print(f"\nERROR: Failed to capture images for {desc}.")

    if args.bias_frame:
        print("--- Master Bias Creation ---")
        print("Cover the lens completely (Pitch black).")
        print("Ensure no light leaks.")
        input("Press Enter to start...")
        config = Pipeline.load_and_validate_config(args.config)
        # Initialize with specialized settings for Bias
        # Overwrite config to force min exposure
        config["capture"]["exposure_us"] = 100 # Minimal possible
        config["capture"]["gain"] = 1.0
        #TODO CHECK FOR CORRECT CAMERA SETTINGS
#        config["capture"]["auto_exposure"] = False
        app = Pipeline(cfg=config)
        capture_calib_stack(app, 50, "Bias", "master_bias.png")
        sys.exit(0)

    if args.flat_frame:
        print("--- Master Flat Creation ---")
        print("Point camera at an evenly illuminated surface (white wall/t-shirt).")
        print("Ensure brightness is neutral (no shadows).")
        input("Press Enter to start...")
        config = Pipeline.load_and_validate_config(args.config)
        # Use Auto Exposure for Flats to get a mid-grey average
        #TODO CHECK FOR CORRECT CAMERA SETTINGS
#        config["capture"]["auto_exposure"] = True 
        app = Pipeline(cfg=config)
        capture_calib_stack(app, 50, "Flat", "master_flat.png")
        sys.exit(0)

    # --- Handle --create-config argument ---
    if args.create_config:
        config_path = "config.json" # Define a default name
        if os.path.exists(config_path):
            print(f"Configuration file '{config_path}' already exists. No action taken.")
        else:
            try:
                # Use the static method to ensure consistency
                Pipeline.save_json_atomic(config_path, DEFAULTS)
                print(f"Successfully created default configuration file at '{config_path}'.")
                print("Please review and edit this file before running the pipeline.")
            except IOError as e:
                print(f"ERROR: Could not write configuration file to '{config_path}': {e}", file=sys.stderr)
        sys.exit(0) # Exit after handling config creation

    if args.dark_frame:
        try:
            Pipeline.create_master_dark(config_path=args.config)
        except Exception as e:
            logging.exception("Failed to create dark frame")
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)  # ← Non-zero exit code

    if not os.path.exists(args.config):
        print(f"ERROR: Configuration file not found at '{args.config}'.", file=sys.stderr)
        print("Please create one or run with --create-config to generate a default file.", file=sys.stderr)
        sys.exit(1)

    # 1. Load the configuration from the file.
    config = Pipeline.load_and_validate_config(args.config)

    # 2. Create an instance of the main application class.
    app = Pipeline(cfg=config, config_path=args.config, simulate=args.simulate)

    # 3. Run the application.
    app.run()
