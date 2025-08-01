import os
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
from dotenv import load_dotenv


class UTF8EncodingHook:
    """Hook to ensure UTF-8 encoding for Windows compatibility"""
    
    @hook_impl
    def before_context_created(self, context) -> None:
        """Set UTF-8 encoding before any operations"""
        os.environ["PYTHONUTF8"] = "1"
        os.environ["PYTHONIOENCODING"] = "utf-8"
        
        return {}


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """

        # Load environment variables from .env file
        load_dotenv()

        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())
        
        # Explicitly set MinIO credentials from environment variables
        # This ensures the credentials are available to the Spark/JVM process
        minio_user = os.getenv("MINIO_USER")
        minio_password = os.getenv("MINIO_PASSWORD")
        minio_endpoint = os.getenv("MINIO_ENDPOINT")
        
        if minio_user and minio_password and minio_endpoint:
            spark_conf.set("spark.hadoop.fs.s3a.access.key", minio_user)
            spark_conf.set("spark.hadoop.fs.s3a.secret.key", minio_password)
            spark_conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            print(f"✅ MinIO credentials set for Spark: user={minio_user}, endpoint={minio_endpoint}")
        else:
            print("⚠️ MinIO credentials not found in environment variables")

        # Initialise the spark session with additional cleanup configurations
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
            # Additional configurations to prevent process lingering
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            # Ensure proper cleanup on Windows
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Prevent Arrow issues on Windows
            .config("spark.driver.maxResultSize", "2g")  # Prevent memory issues
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
        
        # Set additional cleanup settings
        _spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        # Log Spark process information for debugging
        try:
            spark_context = _spark_session.sparkContext
            python_pid = os.getpid()
            jvm_pid = None
            
            # Try to get JVM process ID
            try:
                # Get the JVM process ID from Spark context
                jvm_pid = spark_context._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            except Exception as e:
                jvm_pid = "unknown"
            
            print(f"🔍 Spark Session Created:")
            print(f"   📍 Python PID: {python_pid}")
            print(f"   📍 JVM PID: {jvm_pid}")
            print(f"   📍 App Name: {spark_context.appName}")
            print(f"   📍 Spark Context ID: {spark_context.sparkUser}@{spark_context.applicationId}")
            
        except Exception as e:
            print(f"⚠️ Could not log Spark process info: {e}")

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog) -> None:
        """Ensure clean start before pipeline run"""
        pass

    @hook_impl
    def after_pipeline_run(self, run_params, run_result, pipeline, catalog) -> None:
        """Clean up Spark resources after pipeline completion"""
        self._cleanup_spark_resources("pipeline completion")

    @hook_impl
    def on_pipeline_error(self, error, run_params, pipeline, catalog) -> None:
        """Clean up Spark resources on pipeline error"""
        self._cleanup_spark_resources("pipeline error")

    def _cleanup_spark_resources(self, context: str) -> None:
        """Comprehensive Spark cleanup to prevent lingering processes"""
        import time
        import gc
        
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                # Get JVM PID for cleanup
                jvm_pid = None
                try:
                    spark_context = spark.sparkContext
                    jvm_pid = spark_context._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
                except Exception:
                    pass
                
                # Check if Spark context is still active
                try:
                    spark_context = spark.sparkContext
                    context_active = spark_context is not None and not spark_context._jsc.sc().isStopped()
                except:
                    context_active = False
                
                # Stop Spark context first if active
                if context_active:
                    try:
                        spark.sparkContext.stop()
                    except Exception:
                        pass
                
                # Stop the Spark session if not already stopped
                try:
                    spark.stop()
                except Exception:
                    pass
                
                # Force terminate the JVM process if we captured its PID
                if jvm_pid:
                    try:
                        import subprocess
                        subprocess.run(
                            ['powershell', '-Command', f'Stop-Process -Id {jvm_pid} -Force'],
                            capture_output=True, text=True, timeout=10, check=False
                        )
                    except Exception:
                        pass
                
                # Force garbage collection
                gc.collect()
                
                # Give some time for cleanup
                time.sleep(2)
                
            else:
                pass  # No active Spark session found
                
        except Exception:
            pass  # Silently handle cleanup errors
            
        # Re-enable Windows process cleanup (external error confirmed not from our code)
        self._windows_process_cleanup()
    
    def _windows_process_cleanup(self) -> None:
        """Additional cleanup for Windows-specific process issues"""
        import time
        
        try:
            # Brief wait for natural process termination
            # Note: External "processo não foi encontrado" errors are from Windows system services,
            # not from our cleanup code (confirmed by testing)
            time.sleep(2)
            
        except Exception:
            # Silently handle any cleanup issues
            pass
