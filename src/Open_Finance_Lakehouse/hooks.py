import os
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


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

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog) -> None:
        """Ensure clean start before pipeline run"""
        pass

    @hook_impl
    def after_pipeline_run(self, run_params, run_result, pipeline, catalog) -> None:
        """Pipeline completion - cleanup handled by enhanced hooks"""
        pass

    @hook_impl
    def on_pipeline_error(self, error, run_params, pipeline, catalog) -> None:
        """Pipeline error - cleanup handled by enhanced hooks"""
        pass
