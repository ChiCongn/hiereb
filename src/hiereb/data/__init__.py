"""Input loading, splitting, and synthetic data generation."""

from hiereb.data.loader import LoadedData, load_events
from hiereb.data.splits import split_events

__all__ = ["LoadedData", "load_events", "split_events"]

