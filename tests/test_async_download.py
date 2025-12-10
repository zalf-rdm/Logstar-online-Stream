import asyncio
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
from logstar_stream.logstar import LogstarClient, do_sensor_mapping, do_column_name_mapping

class TestLogstarClient(unittest.IsolatedAsyncioTestCase):
    async def test_download_station_data(self):
        # Mock response data
        header_response = '{"header": {"date": "Date", "time": "Time", "ch1": "Channel 1"}}'
        data_response = '{"header": {"date": "Date", "time": "Time", "ch1": "Channel 1"}, "data": [{"date": "2023-01-01", "time": "12:00", "ch1": 10.5}]}'

        with patch('aiohttp.ClientSession.get') as mock_get:
            # Setup mock for header request
            mock_resp_header = AsyncMock()
            mock_resp_header.status = 200
            mock_resp_header.text.return_value = header_response
            
            # Setup mock for data request
            mock_resp_data = AsyncMock()
            mock_resp_data.status = 200
            mock_resp_data.text.return_value = data_response

            # Configure side_effect to return mock data
            mock_get.return_value.__aenter__.return_value = mock_resp_data

            conf = {
                "startdate": "2023-01-01",
                "enddate": "2023-01-02",
                "datetime": 1,
                "geodata": 0
            }

            async with LogstarClient("test_api_key") as client:
                df = await client.download_station_data(conf, "test_station")
                
                self.assertIsNotNone(df)
                self.assertFalse(df.empty)
                self.assertEqual(len(df), 1)
                self.assertIn("Channel 1", df.columns)
                self.assertEqual(df.iloc[0]["Channel 1"], 10.5)

    def test_mapping(self):
        mapping = {
            "sensor-mapping": {
                "mapped_station": {"values": ["raw_station"], "measurement-class": "class1"}
            },
            "measurement-classes": {
                "class1": {
                    "regex": ".*",
                    "mapping": {"PrettyName": {"abbreviation": "raw_col"}}
                }
            }
        }
        
        # Test station mapping
        mapped_name = do_sensor_mapping("raw_station", mapping)
        self.assertEqual(mapped_name, "mapped_station")
        
        # Test column mapping
        header = {"col1": "raw_col_123"} # Regex .* matches everything
        # Wait, the regex logic in logstar.py is specific.
        # pattern = re.compile(measurement_class["regex"])
        # r = pattern.match(c_name_remote)
        # if value["abbreviation"] in c_name_remote: ...
        
        # Let's test with a simple case that matches the code logic
        # Code: if value["abbreviation"] in c_name_remote:
        #           r = pattern.match(c_name_remote)
        
        mapping_complex = {
            "sensor-mapping": {
                "station1": {"measurement-class": "weather"}
            },
            "measurement-classes": {
                "weather": {
                    "regex": r"(?P<string>\w+)", # Simple regex capturing a string
                    "mapping": {
                        "Temp": {"abbreviation": "T"}
                    },
                    "position": {}
                }
            }
        }
        
        header = {"0": "T_Air"}
        # "T" is in "T_Air"
        # Regex matches "T_Air", group "string" is "T_Air" (or similar depending on regex)
        # Code: elif r["number"] is None and r["string"] is not None: c_name = "{}_{}".format(name, r["string"])
        # Expected: "Temp_T_Air"
        
        new_header = do_column_name_mapping("station1", header, mapping_complex)
        self.assertIn("0", new_header)
        self.assertEqual(new_header["0"], "Temp_T_Air")

if __name__ == '__main__':
    unittest.main()
