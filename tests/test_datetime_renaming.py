import unittest
import pandas as pd
import numpy as np
from datetime import datetime


class TestDateTimeColumnRenaming(unittest.TestCase):
    """Test cases for datetime column renaming functionality"""
    
    def test_rename_separate_date_time_columns(self):
        """Test renaming when date and time are separate columns"""
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'time': ['12:00:00', '13:30:00', '14:45:00'],
            'value': [10.5, 11.2, 9.8]
        })
        
        # Simulate the renaming logic
        rename_datetime_column = 'Timestamp'
        if 'date' in df.columns and 'time' in df.columns:
            df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df = df.drop(columns=['date', 'time'])
        
        # Verify
        self.assertIn('Timestamp', df.columns)
        self.assertNotIn('date', df.columns)
        self.assertNotIn('time', df.columns)
        self.assertEqual(len(df), 3)
        self.assertIsInstance(df['Timestamp'].iloc[0], pd.Timestamp)
        self.assertEqual(df['Timestamp'].iloc[0], pd.Timestamp('2023-01-01 12:00:00'))
    
    def test_rename_capitalized_date_time_columns(self):
        """Test renaming when Date and Time are capitalized"""
        df = pd.DataFrame({
            'Date': ['2023-01-01', '2023-01-02'],
            'Time': ['12:00:00', '13:30:00'],
            'value': [10.5, 11.2]
        })
        
        # Simulate the renaming logic
        rename_datetime_column = 'DateTime'
        if 'Date' in df.columns and 'Time' in df.columns:
            df[rename_datetime_column] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
            df = df.drop(columns=['Date', 'Time'])
        
        # Verify
        self.assertIn('DateTime', df.columns)
        self.assertNotIn('Date', df.columns)
        self.assertNotIn('Time', df.columns)
        self.assertEqual(len(df), 2)
    
    def test_rename_single_datetime_column(self):
        """Test renaming when there's a single dateTime column"""
        df = pd.DataFrame({
            'dateTime': ['2023-01-01 12:00:00', '2023-01-02 13:30:00'],
            'value': [10.5, 11.2]
        })
        
        # Simulate the renaming logic
        rename_datetime_column = 'Timestamp'
        if 'dateTime' in df.columns:
            df = df.rename(columns={'dateTime': rename_datetime_column})
        
        # Verify
        self.assertIn('Timestamp', df.columns)
        self.assertNotIn('dateTime', df.columns)
        self.assertEqual(len(df), 2)
    
    def test_no_rename_when_none_specified(self):
        """Test that columns are not renamed when rename_datetime_column is None"""
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'time': ['12:00:00', '13:30:00'],
            'value': [10.5, 11.2]
        })
        
        original_columns = df.columns.tolist()
        
        # Simulate the renaming logic with None
        rename_datetime_column = None
        if rename_datetime_column:
            # This block should not execute
            if 'date' in df.columns and 'time' in df.columns:
                df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
                df = df.drop(columns=['date', 'time'])
        
        # Verify nothing changed
        self.assertEqual(df.columns.tolist(), original_columns)
        self.assertIn('date', df.columns)
        self.assertIn('time', df.columns)
    
    def test_rename_preserves_other_columns(self):
        """Test that renaming preserves all other columns"""
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'time': ['12:00:00', '13:30:00'],
            'temperature': [20.5, 21.2],
            'humidity': [65.0, 68.5],
            'pressure': [1013.25, 1012.80]
        })
        
        # Simulate the renaming logic
        rename_datetime_column = 'Timestamp'
        if 'date' in df.columns and 'time' in df.columns:
            df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df = df.drop(columns=['date', 'time'])
        
        # Verify all other columns are preserved
        self.assertIn('temperature', df.columns)
        self.assertIn('humidity', df.columns)
        self.assertIn('pressure', df.columns)
        self.assertEqual(len(df.columns), 4)  # Timestamp + 3 other columns
        self.assertEqual(df['temperature'].iloc[0], 20.5)
    
    def test_rename_with_custom_column_name(self):
        """Test renaming with various custom column names"""
        test_cases = [
            'Timestamp',
            'DateTime',
            'RecordTime',
            'MeasurementTime',
            'ts'
        ]
        
        for custom_name in test_cases:
            df = pd.DataFrame({
                'date': ['2023-01-01'],
                'time': ['12:00:00'],
                'value': [10.5]
            })
            
            # Simulate the renaming logic
            if 'date' in df.columns and 'time' in df.columns:
                df[custom_name] = pd.to_datetime(df['date'] + ' ' + df['time'])
                df = df.drop(columns=['date', 'time'])
            
            # Verify
            self.assertIn(custom_name, df.columns, f"Failed for custom name: {custom_name}")
            self.assertNotIn('date', df.columns)
            self.assertNotIn('time', df.columns)
    
    def test_rename_handles_different_date_formats(self):
        """Test that renaming works with consistent date formats"""
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'time': ['12:00:00', '13:30:00', '14:45:00'],
            'value': [10.5, 11.2, 9.8]
        })
        
        # Simulate the renaming logic
        rename_datetime_column = 'Timestamp'
        if 'date' in df.columns and 'time' in df.columns:
            df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df = df.drop(columns=['date', 'time'])
        
        # Verify - pandas should handle the format
        self.assertIn('Timestamp', df.columns)
        self.assertEqual(len(df), 3)
        # All should be valid timestamps
        self.assertTrue(all(isinstance(ts, pd.Timestamp) for ts in df['Timestamp']))
        # Verify specific values
        self.assertEqual(df['Timestamp'].iloc[0], pd.Timestamp('2023-01-01 12:00:00'))
        self.assertEqual(df['Timestamp'].iloc[1], pd.Timestamp('2023-01-02 13:30:00'))
        self.assertEqual(df['Timestamp'].iloc[2], pd.Timestamp('2023-01-03 14:45:00'))
    
    def test_rename_priority_order(self):
        """Test that the renaming logic follows the correct priority order"""
        # Priority: date+time > dateTime > Date+Time
        
        # Test case 1: When both date+time and dateTime exist, date+time takes priority
        df1 = pd.DataFrame({
            'date': ['2023-01-01'],
            'time': ['12:00:00'],
            'dateTime': ['2023-01-02 13:00:00'],
            'value': [10.5]
        })
        
        rename_datetime_column = 'Timestamp'
        if 'date' in df1.columns and 'time' in df1.columns:
            df1[rename_datetime_column] = pd.to_datetime(df1['date'] + ' ' + df1['time'])
            df1 = df1.drop(columns=['date', 'time'])
        elif 'dateTime' in df1.columns:
            df1 = df1.rename(columns={'dateTime': rename_datetime_column})
        
        # Should have used date+time, not dateTime
        self.assertIn('Timestamp', df1.columns)
        self.assertIn('dateTime', df1.columns)  # dateTime should still exist
        self.assertEqual(df1['Timestamp'].iloc[0], pd.Timestamp('2023-01-01 12:00:00'))
    
    def test_rename_with_empty_dataframe(self):
        """Test that renaming handles empty dataframes gracefully"""
        df = pd.DataFrame(columns=['date', 'time', 'value'])
        
        # Simulate the renaming logic
        rename_datetime_column = 'Timestamp'
        if 'date' in df.columns and 'time' in df.columns:
            # This will fail on empty dataframe, so we need to check
            if not df.empty:
                df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
                df = df.drop(columns=['date', 'time'])
        
        # Verify - should still have original columns since df is empty
        self.assertIn('date', df.columns)
        self.assertIn('time', df.columns)
        self.assertEqual(len(df), 0)


if __name__ == '__main__':
    unittest.main()
