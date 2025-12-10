import asyncio
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
import numpy as np
from logstar_stream.logstar import LogstarClient, do_sensor_mapping


class TestEdgeCasesAndErrorHandling(unittest.IsolatedAsyncioTestCase):
    """Test cases for edge cases and error handling in Logstar client"""
    
    async def test_download_with_hash_replacement(self):
        """Test that '#' characters are replaced with NaN"""
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": [
                {"0": "2023-01-01", "1": "12:00", "2": 10.5},
                {"0": "2023-01-01", "1": "13:00", "2": "#"},
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
                df = await client.download_station_data(conf, "test_station")
                
                # Verify '#' was replaced with NaN
                self.assertIsNotNone(df)
                self.assertEqual(len(df), 3)
                self.assertEqual(df.iloc[0]['value'], 10.5)
                self.assertTrue(pd.isna(df.iloc[1]['value']))
                self.assertEqual(df.iloc[2]['value'], 8.2)
    
    async def test_download_with_empty_data(self):
        """Test handling of empty data response"""
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"},
            "data": []
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
                df = await client.download_station_data(conf, "test_station")
                
                # Should return None for empty data
                self.assertIsNone(df)
    
    async def test_download_with_missing_data_key(self):
        """Test handling when 'data' key is missing from response"""
        data_response = '''{
            "header": {"0": "date", "1": "time", "2": "value"}
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
                df = await client.download_station_data(conf, "test_station")
                
                # Should return None when data key is missing
                self.assertIsNone(df)
    
    async def test_download_with_invalid_json(self):
        """Test handling of invalid JSON response"""
        data_response = 'invalid json {{'
        
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
                df = await client.download_station_data(conf, "test_station")
                
                # Should return None for invalid JSON
                self.assertIsNone(df)
    
    async def test_download_with_http_error(self):
        """Test handling of HTTP error responses"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_resp = AsyncMock()
            mock_resp.status = 404
            mock_get.return_value.__aenter__.return_value = mock_resp
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(conf, "test_station")
                
                # Should return None for HTTP errors
                self.assertIsNone(df)
    
    async def test_download_with_network_exception(self):
        """Test handling of network exceptions"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = Exception("Network error")
            
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(conf, "test_station")
                
                # Should return None for network exceptions
                self.assertIsNone(df)
    
    async def test_column_reordering(self):
        """Test that date/time columns are reordered to the front"""
        data_response = '''{
            "header": {"0": "value1", "1": "value2", "2": "date", "3": "time"},
            "data": [{"0": 10.5, "1": 20.3, "2": "2023-01-01", "3": "12:00"}]
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
                df = await client.download_station_data(conf, "test_station")
                
                # Verify columns are reordered with date/time first
                self.assertIsNotNone(df)
                columns = df.columns.tolist()
                self.assertEqual(columns[0], 'date')
                self.assertEqual(columns[1], 'time')
    
    async def test_geodata_parameter(self):
        """Test that geodata parameter is correctly passed to API"""
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
                "geodata": 1  # Request geodata
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(conf, "test_station")
                
                # Verify the URL was called with geodata=1
                mock_get.assert_called()
                call_args = mock_get.call_args
                url = call_args[0][0]
                self.assertIn('/1', url)  # geodata parameter in URL
    
    async def test_geodata_default_value(self):
        """Test that geodata defaults to 0 when not provided"""
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
                "datetime": 1
                # geodata not provided
            }
            
            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(conf, "test_station")
                
                # Verify the URL was called with geodata=0 (default)
                mock_get.assert_called()
                call_args = mock_get.call_args
                url = call_args[0][0]
                self.assertIn('/0', url)  # default geodata parameter in URL
    
    def test_sensor_mapping_not_found(self):
        """Test sensor mapping when station is not in mapping"""
        mapping = {
            "sensor-mapping": {
                "known_station": {"values": ["station1"], "measurement-class": "basic"}
            }
        }
        
        # Unknown station should return itself
        result = do_sensor_mapping("unknown_station", mapping)
        self.assertEqual(result, "unknown_station")
    
    def test_sensor_mapping_found(self):
        """Test sensor mapping when station is in mapping"""
        mapping = {
            "sensor-mapping": {
                "mapped_name": {"values": ["raw_station_1", "raw_station_2"], "measurement-class": "basic"}
            }
        }
        
        # Known station should return mapped name
        result = do_sensor_mapping("raw_station_1", mapping)
        self.assertEqual(result, "mapped_name")
        
        result = do_sensor_mapping("raw_station_2", mapping)
        self.assertEqual(result, "mapped_name")
    
    async def test_session_not_initialized_error(self):
        """Test that using client without context manager raises error"""
        client = LogstarClient("test_api_key")
        
        conf = {
            "startdate": "2023-01-01",
            "enddate": "2023-01-02",
            "datetime": 1,
            "geodata": 0
        }
        
        # Should raise RuntimeError when session is not initialized
        with self.assertRaises(RuntimeError) as context:
            await client._request_data("http://example.com")
        
        self.assertIn("Session not initialized", str(context.exception))
    
    async def test_url_building(self):
        """Test that URLs are built correctly"""
        async with LogstarClient("my_api_key") as client:
            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-31",
                "datetime": 1,
                "geodata": 0
            }
            
            url = client._build_url("station123", "0", conf)
            
            # Verify URL structure
            self.assertIn("https://logstar-online.de/api", url)
            self.assertIn("my_api_key", url)
            self.assertIn("station123", url)
            self.assertIn("2023-01-01", url)
            self.assertIn("2023-01-31", url)
            self.assertIn("/0/", url)  # channel
            self.assertIn("/1/", url)  # datetime
            self.assertIn("/0", url)   # geodata at end


if __name__ == '__main__':
    unittest.main()
