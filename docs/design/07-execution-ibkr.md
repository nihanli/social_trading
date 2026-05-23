## 7. Execution Layer (Interactive Brokers)

### 7a. API Setup — ib_async (Recommended)

```python
pip install ib_async  # Modern maintained fork of ib_insync
```

**Connection ports:**

| Environment | Connection |
|-------------|-----------|
| TWS Paper Trading | `127.0.0.1:7497` |
| TWS Live | `127.0.0.1:7496` |
| IB Gateway Paper | `127.0.0.1:4002` |
| IB Gateway Live | `127.0.0.1:4001` |

**For production:** Use **IB Gateway Stable** (headless, ~200MB RAM vs TWS ~600MB). Enable "Download open orders on connection" and set memory to 4096MB minimum.[^18]

[^18]: ib-api-reloaded/ib_async:README.md (active maintained fork); verified from ibkrcampus.com/ibkr-api-page/twsapi-doc/

### 7b. Social Trading Execution — Full Pattern

```python
from ib_async import *
import asyncio

class IBKRSocialExecutor:
    """Executes social media momentum signals via Interactive Brokers."""
    
    def __init__(self, paper: bool = True):
        self.ib = IB()
        self.port = 7497 if paper else 7496  # paper vs live
        
    async def connect(self):
        await self.ib.connectAsync('127.0.0.1', self.port, clientId=10)
        print(f"Connected: {self.ib.isConnected()}")
    
    async def execute_signal(self, signal: dict) -> dict:
        """
        Place a bracket order (entry + stop-loss + take-profit) for a signal.
        """
        ticker = signal['ticker']
        direction = signal['signal']  # LONG or SHORT
        action = 'BUY' if direction == 'LONG' else 'SELL'
        
        # 1. Qualify contract
        contract = Stock(ticker, 'SMART', 'USD')
        await self.ib.qualifyContractsAsync(contract)
        
        # 2. Get current price
        self.ib.reqMarketDataType(3)  # Use delayed if no live subscription
        ticker_data = self.ib.reqMktData(contract, '', False, False)
        await asyncio.sleep(1)
        entry_price = ticker_data.last or ticker_data.close
        
        # 3. Pre-trade risk check (what-if order)
        test_order = MarketOrder(action, signal['shares'])
        state = self.ib.whatIfOrder(contract, test_order)
        if float(state.initMarginAfter) < self.MIN_MARGIN:
            return {"status": "REJECTED", "reason": "Insufficient margin"}
        
        # 4. Place bracket order: entry + stop-loss + take-profit
        stop_price   = entry_price * (0.92 if action == 'BUY' else 1.08)  # 8% stop
        profit_price = entry_price * (1.04 if action == 'BUY' else 0.96)  # 4% target
        
        parent = MarketOrder(action, signal['shares'])
        parent.orderId = self.ib.client.getReqId()
        parent.transmit = False                         # Hold until all legs ready
        
        stop = StopOrder('SELL' if action=='BUY' else 'BUY', signal['shares'], stop_price)
        stop.orderId = self.ib.client.getReqId()
        stop.parentId = parent.orderId
        stop.transmit = False
        
        take_profit = LimitOrder('SELL' if action=='BUY' else 'BUY', signal['shares'], profit_price)
        take_profit.orderId = self.ib.client.getReqId()
        take_profit.parentId = parent.orderId
        take_profit.transmit = True   # ← releases entire bracket
        
        trades = [
            self.ib.placeOrder(contract, parent),
            self.ib.placeOrder(contract, stop),
            self.ib.placeOrder(contract, take_profit),
        ]
        
        return {"status": "SUBMITTED", "trades": trades, "entry": entry_price,
                "stop": stop_price, "target": profit_price}
    
    async def close_all_positions_eod(self):
        """Close all social media positions 15 minutes before market close."""
        self.ib.reqGlobalCancel()   # Cancel all pending orders
        for pos in self.ib.positions():
            contract = pos.contract
            close_order = MarketOrder(
                'SELL' if pos.position > 0 else 'BUY',
                abs(pos.position)
            )
            self.ib.placeOrder(contract, close_order)
```

[^19]: erdewit/ib_insync:ib_insync/order.py:167-205 (order types); ib-api-reloaded/ib_async:README.md (verified bracket pattern)

### 7c. Important IBKR Gotchas

| Pitfall | Solution |
|---------|----------|
| `transmit=True` on parent bracket before children sent | Always `transmit=False` on parent and all-but-last child |
| Market data type not set → silent no-data | Call `ib.reqMarketDataType(3)` for delayed data |
| Client ID conflict | Each process needs unique `clientId` (0–31) |
| Contract disambiguation | Always call `ib.qualifyContracts()` before trading |
| TWS auto-updates break strategies | Use **offline TWS** for production |
| Historical data pacing throttle | Max 60 requests per 10-minute window |

### 7d. IBKR Rate Limits

| Category | Limit |
|----------|-------|
| Simultaneous market data lines | 100 (shared with TWS GUI) |
| Historical data requests | Max 50 open, 60 per 10-minute window |
| API client connections | Max 32 simultaneous |
| Order rate | ~50 orders/sec practical limit |

---

---

*[⬆ Back to main index](README.md)*
