import requests
from typing import Dict, Any, Optional
import streamlit as st
from config import Config

class OrchAPIClient:
    def __init__(self, base_url: str = Config.ORCH_API_BASE_URL):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
    
    def start_simulation(self, 
                        duration_seconds: int,
                        algorithm: str,
                        algo_consts: Dict[str, Any],
                        simulator_consts: Dict[str, Any]) -> Dict[str, Any]:
        """Start a new simulation"""
        payload = {
            "duration_seconds": duration_seconds,
            "algorithm": algorithm,
            "algo_consts": algo_consts,
            "simulator_consts": simulator_consts
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/simulate/start",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to start simulation: {str(e)}")
    
    def get_simulation_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a specific simulation"""
        try:
            response = self.session.get(
                f"{self.base_url}/simulate/status/{run_id}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to get simulation status: {str(e)}")
    
    def get_all_simulations_status(self) -> Dict[str, Any]:
        """Get status of all simulations"""
        try:
            response = self.session.get(
                f"{self.base_url}/simulate/status",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to get simulations status: {str(e)}")
    
    def get_simulation_runs(self, limit: int = 10, status: Optional[str] = None) -> Dict[str, Any]:
        """Get list of simulation runs"""
        params = {"limit": limit}
        if status:
            params["status"] = status
            
        try:
            response = self.session.get(
                f"{self.base_url}/simulate/runs",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to get simulation runs: {str(e)}")
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=5
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Health check failed: {str(e)}")

@st.cache_resource
def get_api_client():
    """Get cached API client instance"""
    return OrchAPIClient()