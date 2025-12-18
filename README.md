# Fault-Tolerant Distributed Transactions System

A distributed transaction system implementing Two-Phase Commit (2PC), Strict Two-Phase Locking (S2PL), and crash recovery for ICS 2403.

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+


### 1. Setup Virtual Environment
```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt