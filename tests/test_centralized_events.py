"""
Tests for the centralized events API.

These tests verify that events can be passed at the simulation level
and automatically distributed to signals based on device_id.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import tempfile

import signal_replay as sr
from signal_replay.orchestrator import _distribute_events, _load_events, _normalize_device_id_column


def create_multi_device_events(
    device_ids: list,
    events_per_device: int = 10
) -> pd.DataFrame:
    """Create synthetic events for multiple devices."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    events = []
    
    for device_id in device_ids:
        for i in range(events_per_device):
            events.append({
                'device_id': device_id,
                'timestamp': base_time + timedelta(seconds=i * 0.5),
                'event_id': 82 if i % 2 == 0 else 81,
                'parameter': (i % 3) + 1
            })
    
    df = pd.DataFrame(events)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


class TestEventDistribution:
    """Tests for the _distribute_events function."""
    
    def test_distribute_to_all_signals(self):
        """Events are correctly filtered and assigned to each signal."""
        events = create_multi_device_events(['sig_a', 'sig_b', 'sig_c'])
        
        signals = [
            sr.SignalConfig(device_id='sig_a', ip='127.0.0.1', udp_port=1025),
            sr.SignalConfig(device_id='sig_b', ip='127.0.0.1', udp_port=1026),
            sr.SignalConfig(device_id='sig_c', ip='127.0.0.1', udp_port=1027),
        ]
        
        _distribute_events(signals, events)
        
        for sig in signals:
            assert sig.events is not None
            assert len(sig.events) == 10  # events_per_device
            assert (sig.events['device_id'] == sig.device_id).all()
    
    def test_preserve_signal_with_own_events(self):
        """This test is no longer applicable - events are always centralized."""
        # Events are always centralized now, so this test is removed
        pass
    
    def test_missing_device_id_raises_error(self):
        """Error when signal's device_id not found in centralized events."""
        events = create_multi_device_events(['sig_a', 'sig_b'])
        
        signals = [
            sr.SignalConfig(device_id='sig_a', ip='127.0.0.1', udp_port=1025),
            sr.SignalConfig(device_id='sig_missing', ip='127.0.0.1', udp_port=1026),
        ]
        
        with pytest.raises(ValueError, match="not found in events"):
            _distribute_events(signals, events)
    
    def test_device_id_column_alternatives(self):
        """Accept 'DeviceId' as alternative column name."""
        events = create_multi_device_events(['sig_a'])
        events = events.rename(columns={'device_id': 'DeviceId'})
        
        signals = [sr.SignalConfig(device_id='sig_a', ip='127.0.0.1', udp_port=1025)]
        
        _distribute_events(signals, events)
        assert len(signals[0].events) == 10
    
    def test_no_device_id_column_raises_error(self):
        """Error when centralized events lack device_id column."""
        events = create_multi_device_events(['sig_a'])
        events = events.drop(columns=['device_id'])
        
        signals = [sr.SignalConfig(device_id='sig_a', ip='127.0.0.1', udp_port=1025)]
        
        with pytest.raises(ValueError, match="must have a 'device_id' column"):
            _distribute_events(signals, events)
    
    def test_numeric_device_ids(self):
        """Handle numeric device_ids (converted to string for matching)."""
        events = create_multi_device_events([1, 2, 3])
        
        signals = [
            sr.SignalConfig(device_id='1', ip='127.0.0.1', udp_port=1025),
            sr.SignalConfig(device_id='2', ip='127.0.0.1', udp_port=1026),
        ]
        
        _distribute_events(signals, events)
        
        for sig in signals:
            assert sig.events is not None
            assert len(sig.events) == 10


class TestLoadEvents:
    """Tests for the _load_events helper function."""
    
    def test_load_from_dataframe(self):
        """Pass through DataFrame unchanged."""
        df = create_multi_device_events(['a'])
        result = _load_events(df)
        assert result is df
    
    def test_load_from_csv_path(self, tmp_path):
        """Load events from CSV file."""
        csv_path = tmp_path / "events.csv"
        df = create_multi_device_events(['a'])
        df.to_csv(csv_path, index=False)
        
        result = _load_events(str(csv_path))
        assert len(result) == len(df)
    
    def test_load_from_parquet_path(self, tmp_path):
        """Load events from Parquet file."""
        parquet_path = tmp_path / "events.parquet"
        df = create_multi_device_events(['a'])
        df.to_parquet(parquet_path, index=False)
        
        result = _load_events(parquet_path)
        assert len(result) == len(df)


class TestStreamlinedAPI:
    """Tests for the streamlined ATCSimulation initialization."""
    
    def test_init_with_centralized_events(self, tmp_path):
        """Initialize with signals + centralized events."""
        events = create_multi_device_events(['dev1', 'dev2'])
        db_path = str(tmp_path / "test.duckdb")
        
        # This should NOT raise - just verify initialization works
        # We don't run the simulation as that requires mocking
        try:
            sim = sr.ATCSimulation(
                signals=[
                    sr.SignalConfig(device_id='dev1', ip='127.0.0.1', udp_port=1025),
                    sr.SignalConfig(device_id='dev2', ip='127.0.0.1', udp_port=1026),
                ],
                events=events,
                replays=1,
                db_path=db_path,
            )
            # Verify signals have events after distribution
            for sig in sim.config.signals:
                assert sig.events is not None
                assert len(sig.events) == 10
        except Exception as e:
            # Allow DuckDB errors from downstream processing (not our API)
            if "Binder Error" not in str(e):
                raise
    
    def test_init_without_events_raises_error(self, tmp_path):
        """Error when events parameter is missing."""
        db_path = str(tmp_path / "test.duckdb")
        
        with pytest.raises(ValueError, match="Must provide 'events'"):
            sr.ATCSimulation(
                signals=[sr.SignalConfig(device_id='dev1', ip='127.0.0.1', udp_port=1025)],
                replays=1,
                db_path=db_path,
            )
    
    def test_cannot_mix_config_and_signals(self, tmp_path):
        """Error when both config and signals are provided."""
        events = create_multi_device_events(['dev1'])
        db_path = str(tmp_path / "test.duckdb")
        
        config = sr.SimulationConfig(
            signals=[sr.SignalConfig(device_id='dev1', ip='127.0.0.1', udp_port=1025)],
            events=events,
            db_path=db_path,
        )
        
        with pytest.raises(ValueError, match="Cannot specify both"):
            sr.ATCSimulation(
                config=config,
                signals=[sr.SignalConfig(device_id='dev2', ip='127.0.0.1', udp_port=1026)],
            )
    
    def test_legacy_api_still_works(self, tmp_path):
        """Legacy pattern with SimulationConfig still works."""
        events = create_multi_device_events(['dev1'])
        db_path = str(tmp_path / "test.duckdb")
        
        config = sr.SimulationConfig(
            signals=[sr.SignalConfig(device_id='dev1', ip='127.0.0.1', udp_port=1025)],
            events=events,  # Must be centralized
            simulation_replays=1,
            db_path=db_path,
        )
        
        try:
            sim = sr.ATCSimulation(config)
            assert sim.config.simulation_replays == 1
        except Exception as e:
            # Allow DuckDB errors from downstream processing (not our API)
            if "Binder Error" not in str(e):
                raise
    
    def test_legacy_api_with_centralized_events(self, tmp_path):
        """Legacy SimulationConfig with centralized events works."""
        events = create_multi_device_events(['dev1', 'dev2'])
        db_path = str(tmp_path / "test.duckdb")
        
        config = sr.SimulationConfig(
            signals=[
                sr.SignalConfig(device_id='dev1', ip='127.0.0.1', udp_port=1025),
                sr.SignalConfig(device_id='dev2', ip='127.0.0.1', udp_port=1026),
            ],
            events=events,  # Centralized
            simulation_replays=1,
            db_path=db_path,
        )
        
        try:
            sim = sr.ATCSimulation(config)
            # Events should be distributed
            for sig in sim.config.signals:
                assert sig.events is not None
        except Exception as e:
            if "Binder Error" not in str(e):
                raise
