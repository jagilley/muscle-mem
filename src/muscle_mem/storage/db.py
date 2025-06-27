import os
import pickle
from typing import Dict, List, Optional, Tuple

from .types import Trajectory


# Currently minimal, in-memory, and highly unoptimized
# Suggestions welcome for database implementations
class DB:
    def __init__(self, file_path: Optional[str] = None):
        self.trajectories: Dict[Tuple[str, ...], List[Trajectory]] = {}  # tags -> trajectories
        self.file_path = file_path
        if self.file_path and os.path.exists(self.file_path):
            self.load_from_disk()

    def add_trajectory(self, trajectory: Trajectory):
        key = tuple(trajectory.tags)
        if key not in self.trajectories:
            self.trajectories[key] = []
        self.trajectories[key].append(trajectory)
        
        # Auto-save to disk if file_path is configured
        if self.file_path:
            self.save_to_disk()

    def fetch_trajectories(self, tags: List[str], page: int = 0, pagesize: int = 20) -> List[Trajectory]:
        key = tuple(tags)
        if key not in self.trajectories:
            return []

        candidates = self.trajectories[key]

        # return paged results. Note, may be race condition if trajectories are added while paging.
        return candidates[page * pagesize : (page + 1) * pagesize]

    def save_to_disk(self):
        """Save trajectories to disk using pickle."""
        if not self.file_path:
            return
        
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        with open(self.file_path, 'wb') as f:
            pickle.dump(self.trajectories, f)

    def load_from_disk(self):
        """Load trajectories from disk using pickle."""
        if not self.file_path or not os.path.exists(self.file_path):
            return
        
        # Check if file is empty
        if os.path.getsize(self.file_path) == 0:
            return
            
        with open(self.file_path, 'rb') as f:
            self.trajectories = pickle.load(f)
