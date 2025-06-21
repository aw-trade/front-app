import socket
import json
import threading
import time
from datetime import datetime
from typing import Dict, List, Callable, Optional
from dataclasses import dataclass


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
        self.socket.settimeout(1.0)  # 1 second timeout for receiving
        
        self.subscribed_symbols: List[str] = []
        self.is_running = False
        self.receive_thread: Optional[threading.Thread] = None
        
        # Callbacks for different events
        self.on_market_data: Optional[Callable[[MarketData], None]] = None
        self.on_connection_error: Optional[Callable[[Exception], None]] = None
        
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
            
            print(f"✓ Subscribed to {symbol.upper()}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to subscribe to {symbol}: {e}")
            if self.on_connection_error:
                self.on_connection_error(e)
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
            
            print(f"✓ Unsubscribed from {symbol.upper()}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to unsubscribe from {symbol}: {e}")
            if self.on_connection_error:
                self.on_connection_error(e)
            return False
    
    def _receive_data(self):
        """Background thread to receive market data"""
        while self.is_running:
            try:
                data, addr = self.socket.recvfrom(4096)
                
                # Parse JSON data
                json_data = json.loads(data.decode('utf-8'))
                market_data = MarketData.from_dict(json_data)
                
                # Call the callback if set
                if self.on_market_data:
                    self.on_market_data(market_data)
                else:
                    # Default behavior: print the data
                    self._default_data_handler(market_data)
                    
            except socket.timeout:
                # Timeout is expected, continue
                continue
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON: {e}")
            except Exception as e:
                print(f"❌ Error receiving data: {e}")
                if self.on_connection_error:
                    self.on_connection_error(e)
    
    def _default_data_handler(self, data: MarketData):
        """Default handler for market data"""
        print(f"📊 {data.symbol}: ${data.price:.4f} | "
              f"Vol: {data.volume:.2f} | "
              f"Bid: ${data.bid:.4f} | "
              f"Ask: ${data.ask:.4f} | "
              f"Time: {data.to_readable_time()}")
    
    def start(self):
        """Start receiving data"""
        if self.is_running:
            print("⚠️  Client is already running")
            return
        
        self.is_running = True
        self.receive_thread = threading.Thread(target=self._receive_data, daemon=True)
        self.receive_thread.start()
        print("🚀 Crypto stream client started")
    
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
        
        print("🛑 Crypto stream client stopped")
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.socket.close()


# Example usage and demonstration
def main():
    """Example usage of the CryptoStreamClient"""
    
    # Create client with default data printing (no custom handlers needed)
    client = CryptoStreamClient()
    
    try:
        # Start the client
        client.start()
        
        # Subscribe to some cryptocurrencies
        symbols = ["BTC", "ETH", "ADA", "SOL"]
        for symbol in symbols:
            client.subscribe(symbol)
            time.sleep(0.1)  # Small delay between subscriptions
        
        print(f"\n📡 Listening for data from {len(symbols)} symbols...")
        print("Press Ctrl+C to stop\n")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    finally:
        client.stop()


# Alternative simple usage example
def simple_example():
    """Simple example with default behavior"""
    with CryptoStreamClient() as client:
        # Subscribe to Bitcoin and Ethereum
        client.subscribe("BTC")
        #client.subscribe("ETH")
        
        # Let it run for 30 seconds
        print("Running for 30 seconds...")
        time.sleep(30)


if __name__ == "__main__":
    print("🎯 Choose an example:")
    print("1. Full featured example (press 1)")
    print("2. Simple example (press 2)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "2":
        simple_example()
    else:
        main()