#!/usr/bin/env python3
from pathlib import Path
import sys,runpy
release=Path.home()/'supabase-bridge/releases/v3.0.0'
sys.path.insert(0,str(release))
runpy.run_path(str(release/'agent_bridge.py'),run_name='__main__')
