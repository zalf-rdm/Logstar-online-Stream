import asyncio
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
import numpy as np
from logstar_stream.logstar import LogstarClient, do_sensor_mapping, do_column_name_mapping
from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class MockProcessingStep(ProcessingStep):
    """Mock processing step for testing"""
    ps_name = "MockProcessingStep"
    ps_description = "Mock processing step for testing"
    ERROR_VALUE = float("NaN")
    
    def __init__(self, kwargs=None):
        super().__init__(kwargs or {})
        self.was_called = False
        self.received_station = None
        self.received_df = None
    
    def process(self, df: pd.DataFrame, station: str) -> pd.DataFrame:
        """Process the dataframe - just marks that it was called"""
        self.was_called = True
        self.received_station = station
        self.received_df = df.copy()
        return df


class MockFilterProcessingStep(ProcessingStep):
    """Mock processing step that filters columns"""
    ps_name = "MockFilterProcessingStep"
    ps_description = "Mock processing step that filters columns"
    
    def __init__(self, kwargs=None):
        super().__init__(kwargs or {})
        self.columns_to_keep = kwargs.get('columns', 'date time').split(' ') if kwargs else ['date', 'time']
    
    def process(self, df: pd.DataFrame, station: str) -> pd.DataFrame:
        """Filter to keep only specified columns"""
        available_cols = [col for col in self.columns_to_keep if col in df.columns]
        if not available_cols:
            return pd.DataFrame()
        return df[available_cols]


class MockTransformProcessingStep(ProcessingStep):
    """Mock processing step that transforms data"""
    ps_name = "MockTransformProcessingStep"
    ps_description = "Mock processing step that transforms data"
    ERROR_VALUE = -999.0
    
    def __init__(self, kwargs=None):
        super().__init__(kwargs or {})
    
    def process(self, df: pd.DataFrame, station: str) -> pd.DataFrame:
        """Replace negative values with ERROR_VALUE"""
        for col in df.columns:
            if col not in ['date', 'time', 'Date', 'Time']:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df.loc[df[col] < 0, col] = self.ERROR_VALUE
        return df


class TestDateColumnRenaming(unittest.TestCase):
    """Test cases for date/time column renaming functionality"""
    
    def test_basic_column_renaming_with_date_time(self):
        """Test that date and time columns are preserved during renaming"""
        mapping = {
            "sensor-mapping": {
                "test_station": {"measurement-class": "basic"}
            },
            "measurement-classes": {
                "basic": {
                    "regex": r"(?P<string>\w+)",
                    "mapping": {
                        "Temperature": {"abbreviation": "Temp"}
                    },
                    "position": {}
                }
            }
        }
        
        header = {
            "0": "date",
            "1": "time",
            "2": "Temp_sensor"
        }
        
        new_header = do_column_name_mapping("test_station", header, mapping)
        
        # Date and time should be preserved
        self.assertEqual(new_header["0"], "date")
        self.assertEqual(new_header["1"], "time")
        # Temperature column should be renamed
        self.assertEqual(new_header["2"], "Temperature_Temp_sensor")
    
    def test_date_time_fields_ignored_in_mapping(self):
        """Test that date and time fields are always ignored in mapping"""
        mapping = {
            "sensor-mapping": {
                "weather_station": {"measurement-class": "weather"}
            },
            "measurement-classes": {
                "weather": {
                    "regex": r"(?P<number>\d+)",
                    "mapping": {
                        "AirTemp": {"abbreviation": "T"}
                    },
                    "position": {
                        "1": {"side": "north", "depth": "10"}
                    }
                }
            }
        }
        
        header = {
            "0": "date",
            "1": "time",
            "2": "T1"
        }
        
        new_header = do_column_name_mapping("weather_station", header, mapping)
        
        # Verify date and time are not modified
        self.assertIn("0", new_header)
        self.assertIn("1", new_header)
        self.assertEqual(new_header["0"], "date")
        self.assertEqual(new_header["1"], "time")
    
    def test_column_mapping_with_position_depth(self):
        """Test column mapping with position and depth information"""
        mapping = {
            "sensor-mapping": {
                "soil_station": {"measurement-class": "soil"}
            },
            "measurement-classes": {
                "soil": {
                    "regex": r"BC(?P<number>\d+)",
                    "mapping": {
                        "bulk_conductivity": {"abbreviation": "BC"}
                    },
                    "position": {
                        "1": {"side": "left", "depth": "30"},
                        "2": {"side": "right", "depth": "60"}
                    }
                }
            }
        }
        
        header = {
            "0": "date",
            "1": "time",
            "2": "BC1",
            "3": "BC2"
        }
        
        new_header = do_column_name_mapping("soil_station", header, mapping)
        
        self.assertEqual(new_header["0"], "date")
        self.assertEqual(new_header["1"], "time")
        self.assertEqual(new_header["2"], "bulk_conductivity_left_30_cm")
        self.assertEqual(new_header["3"], "bulk_conductivity_right_60_cm")
    
    def test_column_mapping_station_not_in_mapping(self):
        """Test that header is returned unchanged when station not in mapping"""
        mapping = {
            "sensor-mapping": {
                "known_station": {"measurement-class": "basic"}
            },
            "measurement-classes": {
                "basic": {
                    "regex": r".*",
                    "mapping": {}
                }
            }
        }
        
        header = {"0": "date", "1": "time", "2": "value"}
        
        new_header = do_column_name_mapping("unknown_station", header, mapping)
        
        # Should return original header unchanged
        self.assertEqual(new_header, header)
    
    def test_column_mapping_with_only_abbreviation_flag(self):
        """Test column mapping with only_includes_abbreviation flag"""
        mapping = {
            "sensor-mapping": {
                "simple_station": {"measurement-class": "simple"}
            },
            "measurement-classes": {
                "simple": {
                    "regex": r".*",
                    "mapping": {
                        "Voltage": {
                            "abbreviation": "V",
                            "only_includes_abbreviation": True
                        }
                    },
                    "position": {}
                }
            }
        }
        
        header = {
            "0": "date",
            "1": "time",
            "2": "V"
        }
        
        new_header = do_column_name_mapping("simple_station", header, mapping)
        
        self.assertEqual(new_header["0"], "date")
        self.assertEqual(new_header["1"], "time")
        self.assertEqual(new_header["2"], "Voltage")


class TestProcessingStepsIntegration(unittest.IsolatedAsyncioTestCase):
    """Test cases for processing steps integration in download pipeline"""
    
    async def test_single_processing_step_called(self):
        """Test that a single processing step is called during download"""
        mock_ps = MockProcessingStep()
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "ch1"},
            "data": [
                {"0": "2023-01-01", "1": "12:00", "2": 10.5},
                {"0": "2023-01-01", "1": "13:00", "2": 11.2}
            ]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf, 
                    "test_station",
                    processing_steps=[mock_ps]
                )
                
                # Verify processing step was called
                self.assertTrue(mock_ps.was_called)
                self.assertEqual(mock_ps.received_station, "test_station")
                self.assertIsNotNone(mock_ps.received_df)
                
                # Verify data is still present
                self.assertIsNotNone(df)
                self.assertEqual(len(df), 2)
    
    async def test_multiple_processing_steps_chained(self):
        """Test that multiple processing steps are executed in order"""
        mock_ps1 = MockProcessingStep()
        mock_ps2 = MockProcessingStep()
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": [{"0": "2023-01-01", "1": "12:00", "2": 5.0}]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "test_station",
                    processing_steps=[mock_ps1, mock_ps2]
                )
                
                # Both processing steps should be called
                self.assertTrue(mock_ps1.was_called)
                self.assertTrue(mock_ps2.was_called)
                
                # Both should receive the same station name
                self.assertEqual(mock_ps1.received_station, "test_station")
                self.assertEqual(mock_ps2.received_station, "test_station")
    
    async def test_processing_step_filters_columns(self):
        """Test processing step that filters columns"""
        filter_ps = MockFilterProcessingStep({'columns': 'date time'})
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "temp", "3": "humidity"},
            "data": [{"0": "2023-01-01", "1": "12:00", "2": 20.5, "3": 65.0}]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "test_station",
                    processing_steps=[filter_ps]
                )
                
                # Should only have date and time columns
                self.assertIsNotNone(df)
                self.assertEqual(len(df.columns), 2)
                self.assertIn('date', df.columns)
                self.assertIn('time', df.columns)
    
    async def test_processing_step_transforms_data(self):
        """Test processing step that transforms data values"""
        transform_ps = MockTransformProcessingStep()
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": [
                {"0": "2023-01-01", "1": "12:00", "2": 10.5},
                {"0": "2023-01-01", "1": "13:00", "2": -5.0},
                {"0": "2023-01-01", "1": "14:00", "2": 8.2}
            ]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "test_station",
                    processing_steps=[transform_ps]
                )
                
                # Verify negative value was replaced
                self.assertIsNotNone(df)
                self.assertEqual(len(df), 3)
                self.assertEqual(df.iloc[0]['value'], 10.5)
                self.assertEqual(df.iloc[1]['value'], -999.0)  # Transformed
                self.assertEqual(df.iloc[2]['value'], 8.2)
    
    async def test_processing_steps_with_sensor_mapping(self):
        """Test processing steps work correctly with sensor mapping"""
        mock_ps = MockProcessingStep()
        
        sensor_mapping = {
            "sensor-mapping": {
                "mapped_station": {
                    "values": ["raw_station"],
                    "measurement-class": "basic"
                }
            },
            "measurement-classes": {
                "basic": {
                    "regex": r"(?P<string>\w+)",
                    "mapping": {
                        "Temperature": {"abbreviation": "T"}
                    },
                    "position": {}
                }
            }
        }
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "T_air"},
            "data": [{"0": "2023-01-01", "1": "12:00", "2": 15.5}]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "raw_station",
                    processing_steps=[mock_ps],
                    sensor_mapping=sensor_mapping
                )
                
                # Processing step should receive mapped station name
                self.assertTrue(mock_ps.was_called)
                self.assertEqual(mock_ps.received_station, "mapped_station")
                
                # Verify column was renamed
                self.assertIsNotNone(df)
                self.assertIn('Temperature_T_air', df.columns)
    
    async def test_processing_step_returns_empty_dataframe(self):
        """Test handling when processing step returns empty dataframe"""
        # Create a filter that removes all columns
        filter_ps = MockFilterProcessingStep({'columns': 'nonexistent_column'})
        
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": [{"0": "2023-01-01", "1": "12:00", "2": 10.5}]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "test_station",
                    processing_steps=[filter_ps]
                )
                
                # Should return None when dataframe becomes empty
                self.assertIsNone(df)
    
    async def test_no_processing_steps(self):
        """Test download works correctly with no processing steps"""
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": [{"0": "2023-01-01", "1": "12:00", "2": 10.5}]
        }'''
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.text.return_value = data_response
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(
                    conf,
                    "test_station",
                    processing_steps=[]
                )
                
                # Should work fine without processing steps
                self.assertIsNotNone(df)
                self.assertEqual(len(df), 1)
                self.assertIn('value', df.columns)


if __name__ == '__main__':
    unittest.main()
