import streamlit as st
from typing import Dict, Any
import time
from config import Config
from utils.api_client import get_api_client

st.set_page_config(
    page_title="Trading Simulation Orchestrator",
    page_icon="📈",
    layout="wide"
)

def initialize_session_state():
    """Initialize session state variables"""
    if "simulation_runs" not in st.session_state:
        st.session_state.simulation_runs = []
    if "current_run_id" not in st.session_state:
        st.session_state.current_run_id = None

def render_algorithm_config(algorithm: str) -> Dict[str, Any]:
    """Render algorithm-specific configuration form"""
    st.subheader("Algorithm Configuration")
    
    default_config = Config.DEFAULT_ALGO_CONFIG.get(algorithm, {})
    config = {}
    
    if algorithm == "order-book-algo":
        config["IMBALANCE_THRESHOLD"] = st.slider(
            "Imbalance Threshold",
            min_value=0.1,
            max_value=1.0,
            value=default_config.get("IMBALANCE_THRESHOLD", 0.6),
            step=0.1,
            help="Minimum order book imbalance required to trigger a signal"
        )
        
        config["MIN_VOLUME_THRESHOLD"] = st.number_input(
            "Min Volume Threshold",
            min_value=1.0,
            max_value=1000.0,
            value=default_config.get("MIN_VOLUME_THRESHOLD", 10.0),
            help="Minimum volume required to consider a signal"
        )
        
        config["LOOKBACK_PERIODS"] = st.number_input(
            "Lookback Periods",
            min_value=1,
            max_value=50,
            value=default_config.get("LOOKBACK_PERIODS", 5),
            help="Number of periods to look back for signal calculation"
        )
        
        config["SIGNAL_COOLDOWN_MS"] = st.number_input(
            "Signal Cooldown (ms)",
            min_value=10,
            max_value=10000,
            value=default_config.get("SIGNAL_COOLDOWN_MS", 100),
            help="Minimum time between signals in milliseconds"
        )
        
    elif algorithm == "rsi-algo":
        config["RSI_PERIOD"] = st.number_input(
            "RSI Period",
            min_value=2,
            max_value=50,
            value=default_config.get("RSI_PERIOD", 14),
            help="Period for RSI calculation"
        )
        
        config["RSI_OVERBOUGHT"] = st.slider(
            "RSI Overbought Level",
            min_value=50,
            max_value=95,
            value=default_config.get("RSI_OVERBOUGHT", 70),
            help="RSI level considered overbought"
        )
        
        config["RSI_OVERSOLD"] = st.slider(
            "RSI Oversold Level",
            min_value=5,
            max_value=50,
            value=default_config.get("RSI_OVERSOLD", 30),
            help="RSI level considered oversold"
        )
        
        config["SIGNAL_COOLDOWN_MS"] = st.number_input(
            "Signal Cooldown (ms)",
            min_value=10,
            max_value=10000,
            value=default_config.get("SIGNAL_COOLDOWN_MS", 100),
            help="Minimum time between signals in milliseconds"
        )
    
    return config

def render_simulator_config() -> Dict[str, Any]:
    """Render simulator configuration form"""
    st.subheader("Simulator Configuration")
    
    default_config = Config.DEFAULT_SIMULATOR_CONFIG
    config = {}
    
    col1, col2 = st.columns(2)
    
    with col1:
        config["INITIAL_CAPITAL"] = st.number_input(
            "Initial Capital ($)",
            min_value=1000.0,
            max_value=10000000.0,
            value=default_config.get("INITIAL_CAPITAL", 100000.0),
            help="Starting capital for the simulation"
        )
        
        config["POSITION_SIZE_PCT"] = st.slider(
            "Position Size (%)",
            min_value=0.01,
            max_value=1.0,
            value=default_config.get("POSITION_SIZE_PCT", 0.05),
            step=0.01,
            help="Position size as percentage of portfolio"
        )
        
        config["MAX_POSITION_SIZE"] = st.number_input(
            "Max Position Size ($)",
            min_value=100.0,
            max_value=1000000.0,
            value=default_config.get("MAX_POSITION_SIZE", 10000.0),
            help="Maximum position size in dollars"
        )
        
        config["TRADING_FEE_PCT"] = st.number_input(
            "Trading Fee (%)",
            min_value=0.0,
            max_value=1.0,
            value=default_config.get("TRADING_FEE_PCT", 0.001),
            step=0.0001,
            format="%.4f",
            help="Trading fee as percentage of trade value"
        )
    
    with col2:
        config["MIN_CONFIDENCE"] = st.slider(
            "Min Confidence",
            min_value=0.0,
            max_value=1.0,
            value=default_config.get("MIN_CONFIDENCE", 0.3),
            step=0.1,
            help="Minimum confidence required to execute trade"
        )
        
        config["ENABLE_SHORTING"] = st.checkbox(
            "Enable Shorting",
            value=default_config.get("ENABLE_SHORTING", True),
            help="Allow short positions"
        )
        
        config["STATS_INTERVAL_SECS"] = st.number_input(
            "Stats Interval (seconds)",
            min_value=1,
            max_value=300,
            value=default_config.get("STATS_INTERVAL_SECS", 30),
            help="Interval for collecting statistics"
        )
    
    return config

def render_simulation_form():
    """Render the main simulation configuration form"""
    st.title("🎯 Trading Simulation Orchestrator")
    
    # API Health Check
    api_client = get_api_client()
    
    try:
        health = api_client.health_check()
        st.success("✅ API Connection Healthy")
    except Exception as e:
        st.error(f"❌ API Connection Failed: {str(e)}")
        st.stop()
    
    st.header("Start New Simulation")
    
    # Basic Configuration
    col1, col2 = st.columns(2)
    
    with col1:
        algorithm = st.selectbox(
            "Algorithm",
            Config.ALGORITHMS,
            help="Select the trading algorithm to use"
        )
    
    with col2:
        duration_seconds = st.number_input(
            "Duration (seconds)",
            min_value=30,
            max_value=3600,
            value=Config.DEFAULT_DURATION_SECONDS,
            help="How long to run the simulation"
        )
    
    # Algorithm-specific configuration
    algo_config = render_algorithm_config(algorithm)
    
    # Simulator configuration
    simulator_config = render_simulator_config()
    
    # Start simulation button
    if st.button("🚀 Start Simulation", type="primary"):
        with st.spinner("Starting simulation..."):
            try:
                result = api_client.start_simulation(
                    duration_seconds=duration_seconds,
                    algorithm=algorithm,
                    algo_consts=algo_config,
                    simulator_consts=simulator_config
                )
                
                run_id = result.get("run_id")
                st.session_state.current_run_id = run_id
                st.session_state.simulation_runs.append({
                    "run_id": run_id,
                    "algorithm": algorithm,
                    "duration": duration_seconds,
                    "started_at": time.time()
                })
                
                st.success(f"✅ Simulation started successfully!")
                st.info(f"Run ID: `{run_id}`")
                
            except Exception as e:
                st.error(f"❌ Failed to start simulation: {str(e)}")

def render_current_simulation():
    """Render current simulation status"""
    if not st.session_state.current_run_id:
        return
    
    st.header("Current Simulation Status")
    
    run_id = st.session_state.current_run_id
    
    # Import database client
    from utils.db_client import get_simulation_by_id_sync
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.code(f"Run ID: {run_id}")
    
    with col2:
        if st.button("🔄 Refresh Status"):
            pass  # Will trigger rerun
    
    try:
        # Get simulation status directly from database
        status = get_simulation_by_id_sync(run_id)
        
        if not status:
            st.error(f"❌ Simulation {run_id} not found in database")
            return
        
        # Display basic status
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Status", status.get("status", "Unknown"))
        
        with col2:
            st.metric("Algorithm", status.get("algorithm_version", "Unknown"))
            
        with col3:
            duration = status.get("duration_seconds", 0)
            st.metric("Duration", f"{duration}s")
        
        # Show performance metrics if available
        if status.get("net_pnl") is not None:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Net P&L", f"${status['net_pnl']:.2f}")
            with col2:
                if status.get("return_pct") is not None:
                    st.metric("Return", f"{status['return_pct']:.2f}%")
            with col3:
                if status.get("total_trades") is not None:
                    st.metric("Total Trades", status['total_trades'])
        
        # Show status-specific information    
        if status.get("status") == "completed":
            st.success("🎉 Simulation completed successfully!")
            
            # Show detailed results
            if status.get("win_rate") is not None:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Win Rate", f"{status['win_rate']:.2f}%")
                with col2:
                    if status.get("max_drawdown") is not None:
                        st.metric("Max Drawdown", f"{status['max_drawdown']:.2f}%")
            
            if st.button("📊 View Results"):
                st.info("Results viewing will be implemented in the next feature!")
        
        elif status.get("status") == "failed":
            st.error(f"❌ Simulation failed")
            
        elif status.get("status") == "running":
            st.info("⏳ Simulation is currently running...")
            
            # Show progress information if available
            if status.get("start_time"):
                from datetime import datetime
                try:
                    start_time = datetime.fromisoformat(status["start_time"].replace('Z', '+00:00'))
                    elapsed = (datetime.now() - start_time.replace(tzinfo=None)).total_seconds()
                    duration = status.get("duration_seconds", 0)
                    
                    if duration > 0:
                        progress = min(elapsed / duration, 1.0)
                        st.progress(progress)
                        st.text(f"Elapsed: {elapsed:.0f}s / {duration}s")
                except:
                    pass
            
    except Exception as e:
        st.error(f"❌ Failed to get simulation status: {str(e)}")

def render_recent_simulations():
    """Render recent simulations list"""
    st.header("Recent Simulations")
    
    # Import database client
    from utils.db_client import get_recent_simulations_sync, check_db_health_sync
    
    try:
        # Check database health first
        if not check_db_health_sync():
            st.error("❌ Database connection failed")
            return
        
        # Get recent simulations directly from database
        runs = get_recent_simulations_sync(limit=10)
        
        if not runs:
            st.info("No recent simulations found.")
            return
            
        for run in runs:
            with st.expander(f"Run {run['run_id']} - {run['status']}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.text(f"Algorithm: {run.get('algorithm_version', 'Unknown')}")
                    st.text(f"Status: {run.get('status', 'Unknown')}")
                
                with col2:
                    st.text(f"Duration: {run.get('duration_seconds', 0)}s")
                    if run.get('start_time'):
                        st.text(f"Started: {run['start_time'][:19]}")  # Show date/time without microseconds
                    else:
                        st.text("Started: Unknown")
                
                with col3:
                    # Show performance metrics if available
                    if run.get('net_pnl') is not None:
                        st.text(f"Net P&L: ${run['net_pnl']:.2f}")
                    if run.get('return_pct') is not None:
                        st.text(f"Return: {run['return_pct']:.2f}%")
                    if run.get('total_trades') is not None:
                        st.text(f"Trades: {run['total_trades']}")
                    
                    if st.button(f"View Details", key=f"view_{run['run_id']}"):
                        st.session_state.current_run_id = run['run_id']
                        st.rerun()
                        
    except Exception as e:
        st.error(f"❌ Failed to get recent simulations: {str(e)}")

def main():
    """Main application function"""
    initialize_session_state()
    
    # Render main components
    render_simulation_form()
    
    st.divider()
    
    # Show current simulation if exists
    if st.session_state.current_run_id:
        render_current_simulation()
        st.divider()
    
    # Show recent simulations
    render_recent_simulations()

if __name__ == "__main__":
    main()