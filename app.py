import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import time
import socket
import threading
from typing import Dict, List, Optional
from dataclasses import dataclass
import queue

# Page configuration - MUST BE FIRST STREAMLIT COMMAND
st.set_page_config(
    page_title="Live Crypto Trading Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

@dataclass
class MarketData:
    """Market data structure matching the Rust service"""
    symbol: str
    price: float
    volume: float
    bid: float
    ask: float
    timestamp: int
    exchange: str
    
    @classmethod
    def from_dict(cls, data: dict) -> 'MarketData':
        return cls(**data)
    
    def to_readable_time(self) -> str:
        """Convert timestamp to readable format"""
        return datetime.fromtimestamp(self.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

class CryptoStreamClient:
    """Python client for the Rust crypto streaming service"""
    
    def __init__(self, server_host: str = "127.0.0.1", server_port: int = 8888):
        self.server_address = (server_host, server_port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(1.0)
        
        self.subscribed_symbols: List[str] = []
        self.is_running = False
        self.receive_thread: Optional[threading.Thread] = None
        
        # Data queue for thread-safe communication with Streamlit
        self.data_queue = queue.Queue()
        
    def subscribe(self, symbol: str) -> bool:
        """Subscribe to a cryptocurrency symbol"""
        try:
            request = {
                "action": "start",
                "symbol": symbol.upper()
            }
            
            message = json.dumps(request).encode('utf-8')
            self.socket.sendto(message, self.server_address)
            
            if symbol.upper() not in self.subscribed_symbols:
                self.subscribed_symbols.append(symbol.upper())
            
            return True
            
        except Exception as e:
            print(f"Failed to subscribe to {symbol}: {e}")
            return False
    
    def unsubscribe(self, symbol: str) -> bool:
        """Unsubscribe from a cryptocurrency symbol"""
        try:
            request = {
                "action": "stop",
                "symbol": symbol.upper()
            }
            
            message = json.dumps(request).encode('utf-8')
            self.socket.sendto(message, self.server_address)
            
            if symbol.upper() in self.subscribed_symbols:
                self.subscribed_symbols.remove(symbol.upper())
            
            return True
            
        except Exception as e:
            print(f"Failed to unsubscribe from {symbol}: {e}")
            return False
    
    def _receive_data(self):
        """Background thread to receive market data"""
        while self.is_running:
            try:
                data, addr = self.socket.recvfrom(4096)
                json_data = json.loads(data.decode('utf-8'))
                market_data = MarketData.from_dict(json_data)
                
                # Put data in queue for Streamlit to consume
                self.data_queue.put(market_data)
                    
            except socket.timeout:
                continue
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
            except Exception as e:
                print(f"Error receiving data: {e}")
    
    def start(self):
        """Start receiving data"""
        if self.is_running:
            return
        
        self.is_running = True
        self.receive_thread = threading.Thread(target=self._receive_data, daemon=True)
        self.receive_thread.start()
    
    def stop(self):
        """Stop receiving data"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Unsubscribe from all symbols
        for symbol in self.subscribed_symbols.copy():
            self.unsubscribe(symbol)
        
        if self.receive_thread:
            self.receive_thread.join(timeout=2)
    
    def close(self):
        """Close the client connection"""
        self.stop()
        self.socket.close()
    
    def get_latest_data(self) -> List[MarketData]:
        """Get all available data from queue"""
        data_list = []
        try:
            while True:
                data_list.append(self.data_queue.get_nowait())
        except queue.Empty:
            pass
        return data_list

# Initialize session state
if 'crypto_client' not in st.session_state:
    st.session_state.crypto_client = None
if 'market_data' not in st.session_state:
    st.session_state.market_data = {}
if 'price_history' not in st.session_state:
    st.session_state.price_history = {}
if 'is_streaming' not in st.session_state:
    st.session_state.is_streaming = False

def initialize_client(host: str = "127.0.0.1", port: int = 8888):
    """Initialize the crypto client if not already done"""
    if st.session_state.crypto_client is not None:
        st.session_state.crypto_client.close()
    
    st.session_state.crypto_client = CryptoStreamClient(host, port)
    st.session_state.crypto_client.start()

def update_market_data():
    """Update market data from the stream"""
    if st.session_state.crypto_client:
        new_data = st.session_state.crypto_client.get_latest_data()
        
        for data in new_data:
            # Update current market data
            st.session_state.market_data[data.symbol] = data
            
            # Update price history for charting
            if data.symbol not in st.session_state.price_history:
                st.session_state.price_history[data.symbol] = []
            
            # Keep last 100 price points
            history = st.session_state.price_history[data.symbol]
            history.append({
                'timestamp': datetime.fromtimestamp(data.timestamp / 1000),
                'price': data.price,
                'volume': data.volume,
                'bid': data.bid,
                'ask': data.ask
            })
            
            # Keep only last 100 points
            if len(history) > 100:
                history.pop(0)

# Sidebar
st.sidebar.title("₿ Live Crypto Dashboard")

# Connection settings
st.sidebar.subheader("🔧 Connection Settings")
server_host = st.sidebar.text_input("Server Host", "127.0.0.1")
server_port = st.sidebar.number_input("Server Port", value=8888, min_value=1, max_value=65535)

# Connection button
if st.sidebar.button("🔌 Connect"):
    initialize_client(server_host, server_port)
    st.sidebar.success("Connected to server")

# Available cryptocurrencies
available_cryptos = [
    "BTC", "ETH", "ADA", "SOL", "MATIC", "DOT", "LINK", "UNI",
    "AAVE", "SUSHI", "CRV", "COMP", "MKR", "YFI", "SNX", "BAL"
]

selected_crypto = st.sidebar.selectbox(
    "🪙 Select Cryptocurrency",
    available_cryptos,
    index=0
)

# Stream control
st.sidebar.subheader("📡 Stream Control")

col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("🚀 Start Stream"):
        if st.session_state.crypto_client is None:
            initialize_client(server_host, server_port)
        
        if st.session_state.crypto_client and st.session_state.crypto_client.subscribe(selected_crypto):
            st.session_state.is_streaming = True
            st.sidebar.success(f"Started streaming {selected_crypto}")
        else:
            st.sidebar.error("Failed to start stream")

with col2:
    if st.button("🛑 Stop Stream"):
        if st.session_state.crypto_client:
            st.session_state.crypto_client.unsubscribe(selected_crypto)
            if selected_crypto in st.session_state.market_data:
                del st.session_state.market_data[selected_crypto]
            if selected_crypto in st.session_state.price_history:
                del st.session_state.price_history[selected_crypto]
            st.sidebar.success(f"Stopped streaming {selected_crypto}")

# Auto-refresh control
auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)
if auto_refresh:
    refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 10, 2)

# Main content
st.title("₿ Live Cryptocurrency Trading Dashboard")

# Update data if streaming
if st.session_state.is_streaming and st.session_state.crypto_client:
    update_market_data()

# Display current data
if selected_crypto in st.session_state.market_data:
    data = st.session_state.market_data[selected_crypto]
    
    # Current price display
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label=f"{selected_crypto} Price",
            value=f"${data.price:.4f}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="Volume",
            value=f"{data.volume:.2f}",
            delta=None
        )
    
    with col3:
        st.metric(
            label="Bid",
            value=f"${data.bid:.4f}",
            delta=None
        )
    
    with col4:
        st.metric(
            label="Ask",
            value=f"${data.ask:.4f}",
            delta=None
        )
    
    with col5:
        spread = data.ask - data.bid
        spread_pct = (spread / data.price) * 100 if data.price > 0 else 0
        st.metric(
            label="Spread",
            value=f"${spread:.4f}",
            delta=f"{spread_pct:.3f}%"
        )
    
    # Price chart
    if selected_crypto in st.session_state.price_history and st.session_state.price_history[selected_crypto]:
        st.subheader(f"📈 {selected_crypto} Live Price Chart")
        
        history = st.session_state.price_history[selected_crypto]
        df = pd.DataFrame(history)
        
        if len(df) > 0:
            fig = go.Figure()
            
            # Price line
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['price'],
                mode='lines',
                name='Price',
                line=dict(color='#00D2FF', width=2),
                fill='tonexty' if len(df) > 1 else None,
                fillcolor='rgba(0, 210, 255, 0.1)'
            ))
            
            # Bid/Ask spread
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['bid'],
                mode='lines',
                name='Bid',
                line=dict(color='green', width=1, dash='dot'),
                opacity=0.7
            ))
            
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['ask'],
                mode='lines',
                name='Ask',
                line=dict(color='red', width=1, dash='dot'),
                opacity=0.7
            ))
            
            fig.update_layout(
                title=f"{selected_crypto} Live Price Movement",
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                height=500,
                showlegend=True,
                hovermode='x unified',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
            )
            
            fig.update_xaxes(gridcolor='rgba(128,128,128,0.2)')
            fig.update_yaxes(gridcolor='rgba(128,128,128,0.2)')
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Volume chart
            st.subheader(f"📊 {selected_crypto} Volume")
            
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=df['timestamp'],
                y=df['volume'],
                name='Volume',
                marker_color='rgba(255, 193, 7, 0.7)'
            ))
            
            fig_vol.update_layout(
                title=f"{selected_crypto} Trading Volume",
                xaxis_title="Time",
                yaxis_title="Volume",
                height=300,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
            )
            
            st.plotly_chart(fig_vol, use_container_width=True)
    
    # Market data table
    st.subheader("📋 Detailed Market Data")
    
    market_info = {
        'Symbol': data.symbol,
        'Price': f"${data.price:.4f}",
        'Volume': f"{data.volume:.2f}",
        'Bid': f"${data.bid:.4f}",
        'Ask': f"${data.ask:.4f}",
        'Exchange': data.exchange,
        'Last Update': data.to_readable_time()
    }
    
    df_info = pd.DataFrame([market_info])
    st.dataframe(df_info, use_container_width=True, hide_index=True)

else:
    # No data available
    st.info(f"No live data available for {selected_crypto}. Click 'Start Stream' to begin receiving data.")
    
    # Show connection status
    st.subheader("🔌 Connection Status")
    
    if st.session_state.crypto_client is None:
        st.error("❌ Not connected to crypto stream server")
        st.info("Click 'Connect' then 'Start Stream' to begin receiving live data.")
    elif st.session_state.is_streaming:
        st.success("✅ Connected and streaming data")
    else:
        st.warning("⚠️ Connected but not streaming. Select a cryptocurrency and start streaming.")

# Multi-symbol dashboard
st.subheader("📊 Multi-Symbol Dashboard")

if st.session_state.market_data:
    # Create summary table of all active symbols
    summary_data = []
    for symbol, data in st.session_state.market_data.items():
        summary_data.append({
            'Symbol': symbol,
            'Price': f"${data.price:.4f}",
            'Volume': f"{data.volume:.2f}",
            'Bid': f"${data.bid:.4f}",
            'Ask': f"${data.ask:.4f}",
            'Spread': f"${data.ask - data.bid:.4f}",
            'Exchange': data.exchange,
            'Last Update': data.to_readable_time()
        })
    
    if summary_data:
        df_summary = pd.DataFrame(summary_data)
        st.dataframe(df_summary, use_container_width=True, hide_index=True)
else:
    st.info("No active streams. Start streaming some cryptocurrencies to see data here.")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    if st.session_state.is_streaming:
        st.success("🟢 Streaming Active")
    else:
        st.error("🔴 Streaming Inactive")

with col2:
    st.info(f"📡 Active Streams: {len(st.session_state.market_data)}")

with col3:
    st.info(f"🕐 Last Update: {datetime.now().strftime('%H:%M:%S')}")

# Auto refresh
if auto_refresh and st.session_state.is_streaming:
    time.sleep(refresh_rate)
    st.rerun()