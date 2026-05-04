# Meteora: Autonomous Night-Sky Observation & Meteor Detection Pipeline

**Meteora** is a professional-grade, multi-threaded Python pipeline designed for continuous night-sky observation, meteor detection, and astronomical event recording. Optimized for Linux Single Board Computers (SBCs) like the Raspberry Pi 5, it transforms a standard camera module into an intelligent, self-healing astronomical observatory.

---

## 1. Core Functionality & Key Features

Meteora is more than just a motion detector; it is a full-stack astronomical pipeline:
- **Continuous Capture:** High-frequency frame acquisition via Picamera2 or OpenCV.
- **Intelligent Detection:** Real-time frame differencing and morphological analysis to isolate meteor streaks from noise.
- **Event Recording:** Automatic 30-second H.264 video buffering and saving when a detection occurs.
- **Scientific Logging:** Every event is accompanied by JSON metadata, including sky quality, alignment metrics, and environmental data.
- **Live Visualization:** A ZMQ-based debug stream and a Flask-based web dashboard for remote monitoring.
- **Multi-threaded Architecture:** Dedicated threads for capture, detection, stacking, health monitoring, and data offload to ensure zero frame loss.

---

## 2. Hardware Requirements

- **Primary Platform:** Raspberry Pi 5 (Recommended for its high CPU throughput) or Pi 4.
- **Camera:** Raspberry Pi Camera Module 3, High Quality Camera, or compatible V4L2 USB/CSI cameras. Global shutter cameras are preferred for high-speed meteor capture.
- **Operating System:** Raspberry Pi OS (64-bit recommended).
- **Storage:** High-endurance microSD card (Class 10 / U3) or USB 3.0 SSD (highly recommended for heavy video recording).
- **Network:** Stable internet connection for NTP time synchronization and SFTP offloading.
- **Optional:**
    - **GPIO LED:** For status signaling (Heartbeat/Recording).
    - **UPS/Power Monitor:** For graceful shutdowns during power loss (connected via GPIO).

---

## 3. Initial Setup & Calibration

### Installation
```bash
sudo apt-get update && sudo apt-get install -y libcap-dev python3-libcamera ffmpeg
pip3 install opencv-python-headless numpy psutil pyexiv2 gpiod pydantic flask paramiko ntplib pyzmq requests pyephem matplotlib fpdf
```

### Calibration Procedure
Before full autonomous operation, a calibration phase is necessary:
1. **Focusing:** Use the `--simulate` mode or the live dashboard preview to adjust lens focus for sharp, pinpoint stars.
2. **Masking:** Define static masks (if needed) in the configuration to ignore terrestrial light sources, chimneys, or swaying trees.
3. **Thresholding:** Observe the "Noise Level" in the dashboard under various sky conditions to set your `base_threshold`.
4. **Orientation:** Update `camera_azimuth`, `camera_altitude`, and your GPS coordinates in the config to assist in precise lunar position calculations.

---

## 4. Command Line Usage

```bash
# Run with default config
python3 meteora_V197-65py --config config.json

# Run in simulation mode (tests pipeline logic without a hardware camera)
python3 meteora_V197-65py --simulate --config config.json

# Reset configuration to factory defaults
python3 meteora_V197-65py --reset-config

# View all command-line arguments
python3 meteora_V197-65py --help
```

---

## 5. How It Works: A 24-Hour Scenario

1. **Afternoon (Standby):** The `SchedulerThread` wakes up. If it's before `start_time`, it stays in an idle heartbeat mode, checking system vitals.
2. **Sunset:** As `start_time` approaches or light levels drop, the system transitions to "Active" mode. The camera initializes and begins tuning exposure and gain.
3. **Night (Observing):** The `CaptureThread` feeds frames into the `DetectionWorkers`. If a meteor is spotted, an `Event` is triggered, saving a 30s H.264 video and high-resolution stills.
4. **Continuous Stacking:** Every few minutes, a timelapse stack is generated, aligning stars to compensate for Earth's rotation.
5. **Moonrise:** The pipeline calculates the moon's position. If it enters the FOV, detection thresholds are increased locally to prevent glare-induced false positives.
6. **Sunrise:** At `end_time`, the system generates "The Watchman's Journal" (PDF) and "The Nightly Masterpiece" (MIP).
7. **Morning:** Data is offloaded via SFTP to a remote server. The `Janitor` cleans up old local files. The system enters low-power standby.

---

## 6. Autonomous Operation & Self-Healing

Meteora is designed for "unattended" remote operation:
- **Thread Watchdog:** Continuously monitors all critical threads. If the Capture or Detection threads hang, the Watchdog force-restarts the pipeline.
- **Camera Recovery:** If the hardware camera fails (e.g., due to a loose CSI cable), the system attempts up to 20 restarts before entering a "Fatal Error" state.
- **NTP Resilience:** Automatically keeps the system clock accurate via NTP. If the network is down, it uses the last known good time.
- **Power Management:** If a low-power signal is detected via GPIO, the system finishes current disk writes and initiates a safe shutdown to prevent SD card corruption.

---

## 7. Intelligent Observation & Moon Awareness

The pipeline is "Moon Aware," using the `pyephem` library to track the lunar cycle:
- **Adaptive Thresholds:** When the moon is in the Field of View (FOV), a `moon_max_penalty` is applied to the sensitivity. This prevents the system from triggering on lens flares or the moon moving through clouds.
- **Lunar Impact Scoring:** The system tracks how much the moon is washing out the sky, adjusting "Sky Quality" scores accordingly.
- **Dynamic Masking:** During alignment, stars near the moon are excluded to prevent tracking errors caused by lunar glare.

---

## 8. Filtering False Positives & Visual Debugging

Meteora uses several layers of logic to ensure high-quality detections:
- **Area Filtering:** `min_area` and `max_area` constraints ignore tiny sensor noise or large moving objects like clouds or planes.
- **Morphological Checks:** Erosion and Dilation verify the "streak" nature of a detection, separating meteors from point-source noise.
- **Debug Levels:**
    - **Level 1:** alignment metrics and noise levels in logs.
    - **Level 2:** Saves "Mosaic" images on disk showing the internal state: Original / Diff / Thresh / Contours.
    - **Level 3:** Full Quad-View live stream over ZMQ for remote real-time tuning.

---

## 9. Bias Mode and Relationship Between Bias Mode and Low-Lux Weight

- **Bias Mode:** Uses a pre-captured `bias_frame` to subtract fixed-pattern sensor noise (hot pixels and banding). This is crucial for long-exposure, high-gain night captures.
- **Low-Lux Weight:** As the sky gets darker (Low-Lux), the system naturally becomes more sensitive to small changes.
- **The Synergy:** Without Bias Mode, high sensitivity in dark conditions would cause the system to trigger constantly on "hot pixels." By subtracting the bias, the system can push the "Low-Lux Weight" much higher, detecting faint meteors that would otherwise be lost in the noise floor.

---

## 10. Advanced Stacking & Adaptive Astro Stretching

Meteora produces professional-grade astronomical timelapses:
- **ECC Alignment:** Stars are aligned across frames using Enhanced Correlation Coefficient (ECC) to compensate for Earth's rotation or slight camera vibration.
- **Stacking Methods:** Supports Mean (for noise reduction), Median (for removing moving objects), and Sum (for deep-sky enhancement).
- **Adaptive Astro Stretching:** A lux-aware algorithm that applies a non-linear stretch to the image. It pulls detail out of the shadows (stars/milky way) while preventing highlights (moon/streetlights) from blowing out. It dynamically adjusts based on the calculated sky brightness.

---

## 11. The Nightly Masterpiece (MIP)

Every night, Meteora maintains a **Maximum Intensity Projection (MIP)**. This "Master Canvas" keeps the brightest pixel from every frame throughout the night.
- **The Result:** A single image showing every meteor, satellite, and star trail captured during the entire session.
- **Persistence:** The MIP is updated in real-time and saved to disk so that even if the system restarts, the "Masterpiece" continues to grow.

---

## 12. The Watchman’s Journal & Hybrid Sky Quality Scoring

At sunrise, a **Morning Report** (PDF) is generated:
- **Key Metrics:** Total detections, sky clarity %, and storage usage.
- **Hybrid Sky Quality Scoring:** A complex metric (0-100) combining:
    - **Star Count:** How many stars are currently detectable.
    - **Transparency:** The standard deviation of the background (lower is usually clearer).
    - **Lunar Interference:** Penalty based on moon phase and position.
- **Atmosphere Fingerprint:** A summary of whether the night was "Pristine," "Cloudy," or "Turbulent" based on the stability of the sky score.

---

## 13. Data Offload, Archiving & Maintenance

- **SFTP Archiving:** Automatically moves videos and logs to a remote server at the end of the night, ensuring the local SBC never runs out of space.
- **The Janitor:** Monitors disk space. If the drive exceeds the `threshold` (e.g., 90%), it deletes the oldest data.
- **Maintenance Mode:** Users can pause the pipeline via the dashboard to perform physical maintenance (like cleaning the lens) without triggering the Watchdog or generating false "event" recordings.

---

## 14. Memory Management & Safety Mechanisms

- **Dynamic RAM Buffering:** Frames are stored in a circular queue. To prevent Out-of-Memory (OOM) crashes, the system drops frames if RAM usage exceeds `ram_limit_percent`.
- **Atomic Writes:** All images and JSON files are written using temporary files and then moved, preventing corruption during a power failure.
- **Safe Directory Validation:** On startup, the system ensures all output directories are writable and creates them if they are missing.

---

## 15. The Interactive Dashboard

The Flask-based web interface provides:
- **Live Preview:** Low-latency view of the sky.
- **Real-time Stats:** CPU temperature, RAM usage, noise levels, and detection counts.
- **Remote Config:** Change sensitivity, exposure, or stacking methods on the fly.
- **Gallery:** Browse, view, and download recent events, timelapses, and the Morning Report.

---

## 16. Key Metrics

- **Efficiency:** `(Total Triggers - Rejected Noise) / Total Triggers`. High efficiency means the tuning is well-optimized for the local environment.
- **Duty Cycle:** The percentage of time the system spent capturing vs. recovering.
- **Sky Transparency:** Derived from the peak intensity of detected stars.

---

## 17. Configuration Tuning: Detailed DEFAULTS

The `DEFAULTS` dictionary defines the baseline behavior. Here is a breakdown of critical keys:

### `capture`
| Key | Default | Description |
| :--- | :--- | :--- |
| `exposure_us` | 200000 | Baseline exposure in microseconds (0.2s). |
| `gain` | 8.0 | Sensor gain multiplier. |
| `target_sky_brightness` | 15 | Target median brightness (0-255) for Auto-Exposure. |
| `moon_max_penalty` | 40 | Penalty added to threshold when the moon is in FOV. |

### `detection`
| Key | Default | Description |
| :--- | :--- | :--- |
| `min_area` | 50 | Minimum pixel size of a meteor streak. |
| `base_threshold` | 15 | Sensitivity floor for frame differencing. |
| `noise_factor` | 2.5 | Multiplier for dynamic noise adaptation. |
| `auto_sensitivity` | False | If True, the system adjusts threshold based on real-time noise. |

### `timelapse`
| Key | Default | Description |
| :--- | :--- | :--- |
| `stack_N` | 20 | Number of frames to combine into one stack. |
| `stack_align` | True | Enables ECC star alignment. |
| `astro_stretch` | True | Enables the non-linear "Astro" visual enhancement. |

### `janitor`
| Key | Default | Description |
| :--- | :--- | :--- |
| `threshold` | 90.0 | Disk usage percentage (%) that triggers cleanup. |
| `target` | 75.0 | Target disk usage (%) after cleanup. |

---

## 18. Resolution-Dependent Tuning Profiles

Meteora automatically scales its internal logic based on the camera resolution to balance sensitivity and RAM performance:

| Profile | Description | Scaling Logic |
| :--- | :--- | :--- |
| **0.5 MP** | VGA | `max_q`: 1000. Optimized for high FPS on low-power hardware. |
| **1.0 MP** | 720p | `min_area`: 60. The standard balance for Pi 4/5. |
| **2.0 MP** | 1080p | `min_area`: 180. Increased noise suppression for high-res sensors. |
| **3.0 MP** | 2K | `max_q`: 250. Strict RAM limits to prevent OOM. |
| **12.3 MP** | 4K | `min_area`: 700. Large kernels (7x9) used for detecting streaks in ultra-high res. |

---

*“Vigilance under the stars.”*
