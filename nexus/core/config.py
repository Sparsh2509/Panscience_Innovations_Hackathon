"""
Configuration constants for NEXUS platform.
"""

# Default duration (in seconds) for time-bounded worker leases
DEFAULT_LEASE_DURATION_SECONDS: float = 10.0

# Default maximum retry attempts for failed jobs
DEFAULT_MAX_RETRIES: int = 3

# Default priority for jobs (higher numbers indicate higher priority)
DEFAULT_JOB_PRIORITY: int = 0
