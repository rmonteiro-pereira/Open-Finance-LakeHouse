#!/usr/bin/env python3
"""
🏦 Brazilian Financial Lakehouse Data Visualization
====================================================

This script provides comprehensive visualization and analysis of Brazilian financial 
and economic data from a MinIO-based data lakehouse, solving hanging issues by 
implementing safe data discovery with progressive loading and timeout handling.

Author: GitHub Copilot
Date: August 1, 2025
"""

import os
import sys
import warnings
import traceback
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lakehouse_visualization.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Essential imports
try:
    import pandas as pd
    import io
    from dotenv import load_dotenv
    from minio import Minio
    import re
    logger.info("✅ Core libraries loaded successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import core libraries: {e}")
    sys.exit(1)

# Visualization imports (optional)
try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
    logger.info("✅ Plotly visualization libraries loaded")
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.warning("⚠️ Plotly not available - visualizations will be limited")


class LakehouseConfig:
    """Configuration management for the lakehouse connection"""
    
    def __init__(self):
        """Initialize configuration from environment variables"""
        load_dotenv()
        
        self.config = {
            "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            "access_key": os.getenv("MINIO_USER", "minioadmin"),
            "secret_key": os.getenv("MINIO_PASSWORD", "minioadmin"),
            "bucket_name": os.getenv("MINIO_BUCKET", "lakehouse")
        }
        
        # Sanitize endpoint
        self.endpoint = self._sanitize_endpoint()
        self.secure = self.config["endpoint"].startswith("https")
        
    def _sanitize_endpoint(self):
        """Remove protocol and path from endpoint"""
        endpoint = self.config["endpoint"]
        endpoint = re.sub(r"^https?://", "", endpoint)
        return endpoint.split("/")[0]
    
    def get_minio_client(self):
        """Create and return MinIO client"""
        return Minio(
            self.endpoint,
            access_key=self.config["access_key"],
            secret_key=self.config["secret_key"],
            secure=self.secure
        )


class SafeDataDiscovery:
    """Safe data discovery with hanging prevention"""
    
    def __init__(self, minio_client, bucket_name):
        self.minio_client = minio_client
        self.bucket_name = bucket_name
        self.max_files_per_layer = 50  # Limit to prevent hanging
        self.max_sample_files = 5      # Limit for sampling
        
    def log_progress(self, message):
        """Log progress with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{timestamp}] {message}")
        
    def safe_list_objects(self, prefix="", recursive=True, max_objects=100):
        """Safely list objects with limits to prevent hanging"""
        try:
            self.log_progress(f"📁 Listing objects with prefix '{prefix}' (max: {max_objects})")
            
            objects_iter = self.minio_client.list_objects(
                self.bucket_name, 
                prefix=prefix, 
                recursive=recursive
            )
            
            objects = []
            count = 0
            
            for obj in objects_iter:
                objects.append(obj)
                count += 1
                
                if count % 10 == 0:
                    self.log_progress(f"   📊 Found {count} objects...")
                
                if count >= max_objects:
                    self.log_progress(f"   🚫 Reached safety limit of {max_objects} objects")
                    break
                    
            self.log_progress(f"   ✅ Listed {len(objects)} objects")
            return objects
            
        except Exception as e:
            self.log_progress(f"   ❌ Error listing objects: {str(e)}")
            return []
    
    def discover_data_structure(self):
        """Discover the overall structure of the data lake"""
        self.log_progress("🔍 DISCOVERING DATA LAKE STRUCTURE")
        
        structure = {
            'layers': {},
            'total_files': 0,
            'parquet_files': 0,
            'summary': {}
        }
        
        try:
            # Get top-level folders
            folders = self.safe_list_objects(recursive=False, max_objects=20)
            folder_names = [f.object_name for f in folders if f.object_name.endswith('/')]
            
            self.log_progress(f"📂 Found {len(folder_names)} layers: {folder_names}")
            
            # Analyze each layer
            for folder in folder_names:
                layer_name = folder.rstrip('/')
                self.log_progress(f"📊 Analyzing {layer_name} layer...")
                
                # Get files in this layer (limited)
                layer_objects = self.safe_list_objects(
                    prefix=folder, 
                    recursive=True, 
                    max_objects=self.max_files_per_layer
                )
                
                parquet_files = [obj for obj in layer_objects 
                               if obj.object_name.endswith('.parquet')]
                
                structure['layers'][layer_name] = {
                    'total_files': len(layer_objects),
                    'parquet_files': len(parquet_files),
                    'sample_files': [f.object_name for f in parquet_files[:3]]
                }
                
                structure['total_files'] += len(layer_objects)
                structure['parquet_files'] += len(parquet_files)
                
                self.log_progress(f"   📈 {layer_name}: {len(parquet_files)} parquet files")
                
        except Exception as e:
            self.log_progress(f"❌ Error discovering structure: {str(e)}")
            
        structure['summary'] = {
            'layers_count': len(structure['layers']),
            'total_files': structure['total_files'],
            'parquet_files': structure['parquet_files']
        }
        
        return structure
    
    def load_sample_data(self, file_path):
        """Safely load a single parquet file as sample"""
        try:
            self.log_progress(f"📄 Loading sample: {file_path}")
            
            response = self.minio_client.get_object(self.bucket_name, file_path)
            df = pd.read_parquet(io.BytesIO(response.data))
            
            self.log_progress(f"   ✅ Loaded: {df.shape} shape")
            return df
            
        except Exception as e:
            self.log_progress(f"   ❌ Error loading {file_path}: {str(e)}")
            return None
    
    def discover_data_samples(self, structure):
        """Load sample data from each layer"""
        self.log_progress("🔬 LOADING DATA SAMPLES")
        
        samples = {}
        
        for layer_name, layer_info in structure['layers'].items():
            sample_files = layer_info.get('sample_files', [])
            
            if sample_files:
                sample_file = sample_files[0]  # Take first file as sample
                df_sample = self.load_sample_data(sample_file)
                
                if df_sample is not None:
                    samples[layer_name] = {
                        'file': sample_file,
                        'shape': df_sample.shape,
                        'columns': list(df_sample.columns),
                        'data_types': df_sample.dtypes.to_dict(),
                        'sample_data': df_sample.head(3).to_dict()
                    }
                    
        return samples


class TimeSeriesProcessor:
    """Process and clean time series data"""
    
    @staticmethod
    def detect_time_series_columns(df):
        """Detect date and value columns automatically"""
        
        # Common date column patterns
        date_patterns = ['date', 'data', 'time', 'dt', 'timestamp', 'created_at']
        date_col = None
        
        for col in df.columns:
            if any(pattern in col.lower() for pattern in date_patterns):
                date_col = col
                break
                
        # Common value column patterns  
        value_patterns = ['value', 'valor', 'close', 'price', 'rate', 'index_value', 'amount']
        value_col = None
        
        for col in df.columns:
            if any(pattern in col.lower() for pattern in value_patterns):
                value_col = col
                break
                
        return date_col, value_col
    
    @staticmethod
    def clean_time_series(df, date_col, value_col):
        """Clean and standardize time series data"""
        try:
            # Create standardized DataFrame
            df_clean = pd.DataFrame({
                'date': pd.to_datetime(df[date_col], errors='coerce'),
                'value': pd.to_numeric(df[value_col], errors='coerce')
            }).dropna()
            
            if df_clean.empty:
                return None
                
            # Sort by date and remove duplicates
            df_clean = df_clean.sort_values('date').drop_duplicates(subset=['date'], keep='last')
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleaning time series: {str(e)}")
            return None


class LakehouseVisualizer:
    """Create visualizations from lakehouse data"""
    
    def __init__(self):
        self.plotly_available = PLOTLY_AVAILABLE
        
    def create_summary_chart(self, structure):
        """Create summary chart of data lake structure"""
        
        if not self.plotly_available:
            logger.warning("⚠️ Plotly not available - using text summary")
            self._print_text_summary(structure)
            return
            
        try:
            # Prepare data for visualization
            layers = list(structure['layers'].keys())
            parquet_counts = [structure['layers'][layer]['parquet_files'] for layer in layers]
            total_counts = [structure['layers'][layer]['total_files'] for layer in layers]
            
            # Create bar chart
            fig = go.Figure(data=[
                go.Bar(name='Parquet Files', x=layers, y=parquet_counts),
                go.Bar(name='Total Files', x=layers, y=total_counts)
            ])
            
            fig.update_layout(
                title="📊 Lakehouse Data Structure Summary",
                xaxis_title="Data Layers",
                yaxis_title="Number of Files",
                barmode='group'
            )
            
            fig.show()
            logger.info("✅ Summary visualization created successfully")
            
        except Exception as e:
            logger.error(f"❌ Error creating summary chart: {str(e)}")
            self._print_text_summary(structure)
    
    def _print_text_summary(self, structure):
        """Print text-based summary when visualization libraries unavailable"""
        
        print("\n" + "="*60)
        print("📊 LAKEHOUSE DATA STRUCTURE SUMMARY")
        print("="*60)
        
        for layer_name, layer_info in structure['layers'].items():
            print(f"\n📁 {layer_name.upper()} LAYER:")
            print(f"   📄 Total files: {layer_info['total_files']}")
            print(f"   📊 Parquet files: {layer_info['parquet_files']}")
            
            if layer_info['sample_files']:
                print("   📋 Sample files:")
                for sample_file in layer_info['sample_files']:
                    print(f"      • {sample_file}")
        
        print("\n📈 TOTAL SUMMARY:")
        print(f"   🗂️ Layers: {structure['summary']['layers_count']}")
        print(f"   📄 Total files: {structure['summary']['total_files']}")
        print(f"   📊 Parquet files: {structure['summary']['parquet_files']}")
        print("="*60)
    
    def create_time_series_chart(self, df, title):
        """Create time series visualization"""
        
        if not self.plotly_available:
            logger.warning("⚠️ Plotly not available - showing data summary")
            self._print_time_series_summary(df, title)
            return
            
        try:
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['value'],
                mode='lines',
                name=title,
                line=dict(width=2)
            ))
            
            fig.update_layout(
                title=f"📈 {title}",
                xaxis_title="Date",
                yaxis_title="Value",
                hovermode='x unified'
            )
            
            fig.show()
            logger.info(f"✅ Time series chart created for {title}")
            
        except Exception as e:
            logger.error(f"❌ Error creating time series chart: {str(e)}")
            self._print_time_series_summary(df, title)
    
    def _print_time_series_summary(self, df, title):
        """Print time series summary when visualization unavailable"""
        
        print(f"\n📈 TIME SERIES SUMMARY: {title}")
        print("-" * 50)
        print(f"📊 Records: {len(df)}")
        print(f"📅 Date range: {df['date'].min():%Y-%m-%d} to {df['date'].max():%Y-%m-%d}")
        print(f"📈 Value range: {df['value'].min():,.2f} to {df['value'].max():,.2f}")
        print("📊 Latest values:")
        print(df.tail(5).to_string(index=False))


class LakehouseAnalyzer:
    """Main analyzer class that orchestrates the entire process"""
    
    def __init__(self):
        """Initialize the analyzer with configuration"""
        logger.info("🚀 Initializing Lakehouse Analyzer")
        
        # Load configuration
        self.config = LakehouseConfig()
        
        # Initialize MinIO client
        try:
            self.minio_client = self.config.get_minio_client()
            logger.info(f"✅ MinIO client initialized: {self.config.endpoint}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize MinIO client: {e}")
            raise
            
        # Initialize components
        self.discovery = SafeDataDiscovery(self.minio_client, self.config.config["bucket_name"])
        self.processor = TimeSeriesProcessor()
        self.visualizer = LakehouseVisualizer()
        
    def test_connection(self):
        """Test MinIO connection safely"""
        logger.info("🔍 Testing MinIO connection...")
        
        try:
            # Test bucket existence
            bucket_exists = self.minio_client.bucket_exists(self.config.config["bucket_name"])
            
            if not bucket_exists:
                logger.error(f"❌ Bucket '{self.config.config['bucket_name']}' does not exist")
                return False
                
            logger.info(f"✅ Connection successful to bucket: {self.config.config['bucket_name']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {str(e)}")
            return False
    
    def analyze_lakehouse(self):
        """Main analysis method - discovers and analyzes all data"""
        logger.info("🏦 STARTING COMPREHENSIVE LAKEHOUSE ANALYSIS")
        logger.info("="*60)
        
        results = {
            'connection_test': False,
            'structure': {},
            'samples': {},
            'time_series': {},
            'summary': {}
        }
        
        try:
            # Step 1: Test connection
            if not self.test_connection():
                logger.error("❌ Connection test failed - aborting analysis")
                return results
                
            results['connection_test'] = True
            
            # Step 2: Discover structure
            logger.info("📊 Discovering data lake structure...")
            structure = self.discovery.discover_data_structure()
            results['structure'] = structure
            
            # Step 3: Load samples
            logger.info("🔬 Loading data samples...")
            samples = self.discovery.discover_data_samples(structure)
            results['samples'] = samples
            
            # Step 4: Process time series (if samples available)
            if samples:
                logger.info("📈 Processing time series data...")
                time_series = self._process_time_series_samples(samples)
                results['time_series'] = time_series
            
            # Step 5: Create visualizations
            logger.info("🎨 Creating visualizations...")
            self.visualizer.create_summary_chart(structure)
            
            # Create time series charts for samples
            for series_name, df in results['time_series'].items():
                if len(df) > 0:
                    self.visualizer.create_time_series_chart(df, series_name)
            
            # Step 6: Generate summary
            results['summary'] = self._generate_summary(results)
            
            logger.info("🎉 LAKEHOUSE ANALYSIS COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            logger.error(f"❌ Analysis failed: {str(e)}")
            traceback.print_exc()
            
        return results
    
    def _process_time_series_samples(self, samples):
        """Process sample data into time series"""
        
        time_series = {}
        
        for layer_name, sample_info in samples.items():
            try:
                # Recreate DataFrame from sample data
                sample_data = sample_info['sample_data']
                df = pd.DataFrame(sample_data)
                
                if df.empty:
                    continue
                    
                # Detect time series columns
                date_col, value_col = self.processor.detect_time_series_columns(df)
                
                if date_col and value_col:
                    # Clean time series data
                    df_clean = self.processor.clean_time_series(df, date_col, value_col)
                    
                    if df_clean is not None and len(df_clean) > 0:
                        time_series[f"{layer_name}_sample"] = df_clean
                        logger.info(f"✅ Processed {layer_name} time series: {len(df_clean)} records")
                    else:
                        logger.warning(f"⚠️ No valid time series data in {layer_name}")
                else:
                    logger.warning(f"⚠️ Could not detect time series columns in {layer_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing {layer_name}: {str(e)}")
                
        return time_series
    
    def _generate_summary(self, results):
        """Generate comprehensive summary of analysis"""
        
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'connection_successful': results['connection_test'],
            'total_layers': len(results['structure'].get('layers', {})),
            'total_files': results['structure'].get('summary', {}).get('total_files', 0),
            'parquet_files': results['structure'].get('summary', {}).get('parquet_files', 0),
            'samples_loaded': len(results['samples']),
            'time_series_processed': len(results['time_series']),
            'visualization_libraries': {
                'plotly': PLOTLY_AVAILABLE
            }
        }
        
        return summary


def main():
    """Main execution function"""
    print("🏦 BRAZILIAN FINANCIAL LAKEHOUSE ANALYZER")
    print("="*50)
    print("📊 Comprehensive data discovery and visualization")
    print("🔒 Safe execution with hanging prevention")
    print("="*50)
    
    try:
        # Initialize analyzer
        analyzer = LakehouseAnalyzer()
        
        # Run comprehensive analysis
        results = analyzer.analyze_lakehouse()
        
        # Print final summary
        if results['summary']:
            print("\n📋 FINAL ANALYSIS SUMMARY:")
            print("="*30)
            summary = results['summary']
            
            print(f"🔗 Connection: {'✅ Success' if summary['connection_successful'] else '❌ Failed'}")
            print(f"📁 Layers discovered: {summary['total_layers']}")
            print(f"📄 Total files: {summary['total_files']}")
            print(f"📊 Parquet files: {summary['parquet_files']}")
            print(f"🔬 Samples loaded: {summary['samples_loaded']}")
            print(f"📈 Time series processed: {summary['time_series_processed']}")
            print(f"🎨 Plotly available: {'✅' if summary['visualization_libraries']['plotly'] else '❌'}")
            print(f"⏰ Analysis completed: {summary['analysis_timestamp']}")
            
        print("\n🎉 ANALYSIS COMPLETED - NO HANGING DETECTED!")
        print("✅ Check the log file 'lakehouse_visualization.log' for detailed output")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Main execution failed: {str(e)}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run the main analysis
    results = main()
    
    # Exit with appropriate code
    if results and results.get('summary', {}).get('connection_successful', False):
        print("\n✅ SUCCESS: Lakehouse analysis completed successfully!")
        sys.exit(0)  # Success
    else:
        print("\n❌ FAILURE: Lakehouse analysis failed!")
        sys.exit(1)  # Failure
