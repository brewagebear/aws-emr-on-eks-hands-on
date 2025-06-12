#!/bin/bash

# 간단한 EMR Spark on EKS 배포 스크립트 (MWAA 제외)
PROJECT_NAME="spark-on-eks-hands-on"
REGION="ap-northeast-2"

echo "🚀 간단한 EMR Spark on EKS 배포 (MWAA 없음)"
echo "============================================"

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

# 에러 핸들링
set -e
trap 'log_error "스크립트 실행 중 오류가 발생했습니다. 라인 $LINENO에서 중단됨"' ERR

# 사전 검증
check_prerequisites() {
    log_info "사전 요구사항 확인 중..."

    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI가 설치되지 않았습니다."
        echo "설치 방법: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi

    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS 자격 증명이 설정되지 않았습니다."
        echo "다음 명령을 실행하세요: aws configure"
        exit 1
    fi

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    log_success "AWS CLI 및 자격 증명 확인 완료 (계정: $ACCOUNT_ID)"
}

# CloudFormation 템플릿 검증
validate_template() {
    log_info "CloudFormation 템플릿 검증 중..."

    if [ ! -f "emr-spark-eks-infrastructure.yaml" ]; then
        log_error "CloudFormation 템플릿을 찾을 수 없습니다: emr-spark-eks-infrastructure.yaml"
        exit 1
    fi

    aws cloudformation validate-template \
        --template-body file://emr-spark-eks-infrastructure.yaml \
        --region $REGION > /dev/null

    log_success "CloudFormation 템플릿 유효성 검사 통과"
}

# CloudFormation 스택 배포
deploy_stack() {
    log_info "CloudFormation 스택 배포 시작..."

    # 기존 스택 확인
    if aws cloudformation describe-stacks --stack-name $PROJECT_NAME --region $REGION &> /dev/null; then
        log_warning "기존 스택이 존재합니다."
        read -p "기존 스택을 업데이트하시겠습니까? (y/N): " update_choice

        if [[ $update_choice =~ ^[Yy]$ ]]; then
            log_info "스택 업데이트 중..."
            aws cloudformation update-stack \
                --stack-name $PROJECT_NAME \
                --template-body file://emr-spark-eks-infrastructure.yaml \
                --parameters ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME \
                --capabilities CAPABILITY_NAMED_IAM \
                --region $REGION

            log_info "스택 업데이트 완료 대기 중..."
            aws cloudformation wait stack-update-complete \
                --stack-name $PROJECT_NAME \
                --region $REGION
        else
            log_error "배포를 중단합니다."
            exit 1
        fi
    else
        log_info "새로운 CloudFormation 스택 생성 중..."
        aws cloudformation create-stack \
            --stack-name $PROJECT_NAME \
            --template-body file://emr-spark-eks-infrastructure.yaml \
            --parameters ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME \
            --capabilities CAPABILITY_NAMED_IAM \
            --region $REGION \
            --on-failure DELETE

        log_info "스택 생성 중... (약 15-20분 소요)"
        monitor_stack_creation
    fi

    log_success "CloudFormation 스택 배포 완료"
}

# 스택 생성 모니터링
monitor_stack_creation() {
    local start_time=$(date +%s)
    local last_status=""

    while true; do
        stack_status=$(aws cloudformation describe-stacks \
            --stack-name $PROJECT_NAME \
            --query 'Stacks[0].StackStatus' \
            --output text \
            --region $REGION 2>/dev/null)

        if [ "$stack_status" != "$last_status" ]; then
            current_time=$(date +%s)
            elapsed=$((current_time - start_time))
            minutes=$((elapsed / 60))
            echo "   ⏳ ${minutes}분 경과 - 상태: $stack_status"
            last_status="$stack_status"
        fi

        case $stack_status in
            "CREATE_COMPLETE")
                log_success "CloudFormation 스택 생성 완료"
                return 0
                ;;
            "CREATE_FAILED"|"ROLLBACK_COMPLETE"|"ROLLBACK_FAILED")
                log_error "스택 생성 실패: $stack_status"
                echo ""
                echo "실패 원인:"
                aws cloudformation describe-stack-events \
                    --stack-name $PROJECT_NAME \
                    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].{Resource:LogicalResourceId,Reason:ResourceStatusReason}' \
                    --output table \
                    --region $REGION
                return 1
                ;;
            "CREATE_IN_PROGRESS")
                sleep 30
                ;;
            *)
                log_warning "알 수 없는 상태: $stack_status"
                sleep 30
                ;;
        esac
    done
}

# 스택 출력값 수집
collect_outputs() {
    log_info "스택 출력값 수집 중..."

    DATA_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name $PROJECT_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
        --output text \
        --region $REGION)

    LOGS_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name $PROJECT_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`LogsBucketName`].OutputValue' \
        --output text \
        --region $REGION)

    EMR_ROLE_ARN=$(aws cloudformation describe-stacks \
        --stack-name $PROJECT_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRExecutionRoleArn`].OutputValue' \
        --output text \
        --region $REGION)

    CLUSTER_NAME=$(aws cloudformation describe-stacks \
        --stack-name $PROJECT_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`EKSClusterName`].OutputValue' \
        --output text \
        --region $REGION)

    log_success "스택 출력값 수집 완료"
    echo "   데이터 버킷: $DATA_BUCKET"
    echo "   로그 버킷: $LOGS_BUCKET"
    echo "   EKS 클러스터: $CLUSTER_NAME"
}

# 샘플 데이터 및 Spark 스크립트 생성
create_sample_files() {
    log_info "샘플 데이터 및 Spark 스크립트 생성 중..."

    # 샘플 데이터 생성 스크립트
    cat > generate_test_logs.py << 'EOF'
#!/usr/bin/env python3
"""
테스트용 웹 로그 데이터 생성 스크립트
"""
import pandas as pd
import random
import boto3
import sys
from datetime import datetime, timedelta
import io

def generate_web_logs(num_records=10000):
    """웹 로그 데이터 생성"""
    print(f"📊 {num_records}개의 테스트 로그 생성 중...")

    # 샘플 데이터 구성 요소
    ips = [f"192.168.{random.randint(1,10)}.{random.randint(1,254)}" for _ in range(100)]
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    urls = [f'/api/v1/{endpoint}' for endpoint in ['users', 'orders', 'products', 'analytics', 'reports']]
    status_codes = [200, 201, 400, 404, 500, 502]
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)',
        'PostmanRuntime/7.28.4'
    ]

    # 로그 데이터 생성
    data = []
    base_time = datetime.now() - timedelta(hours=24)

    for i in range(num_records):
        # 시간 분포를 현실적으로 (피크 시간대 고려)
        hour_offset = random.randint(0, 1440)  # 24시간 = 1440분
        timestamp = base_time + timedelta(minutes=hour_offset)

        # 상태 코드별 가중치 (성공이 더 많음)
        status_weights = [0.7, 0.1, 0.1, 0.05, 0.03, 0.02]
        status_code = random.choices(status_codes, weights=status_weights)[0]

        # 응답 시간 (상태 코드에 따라 조정)
        if status_code == 200:
            response_time = random.randint(50, 500)
        elif status_code in [400, 404]:
            response_time = random.randint(20, 200)
        else:
            response_time = random.randint(1000, 5000)

        record = {
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'ip_address': random.choice(ips),
            'method': random.choice(methods),
            'url': random.choice(urls),
            'status_code': status_code,
            'response_time_ms': response_time,
            'user_agent': random.choice(user_agents),
            'bytes_sent': random.randint(512, 102400),
            'session_id': f"sess_{random.randint(100000, 999999)}"
        }
        data.append(record)

    return pd.DataFrame(data)

def upload_to_s3(df, bucket_name):
    """DataFrame을 S3에 CSV로 업로드"""
    print(f"📤 S3 버킷에 업로드 중: {bucket_name}")

    s3_client = boto3.client('s3')

    # CSV 변환
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # 파일명에 타임스탬프 포함
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"input/web_logs_{timestamp}.csv"

    # S3 업로드
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    print(f"✅ 업로드 완료: s3://{bucket_name}/{key}")
    return f"s3://{bucket_name}/{key}"

if __name__ == "__main__":
    # 인자 파싱
    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    bucket_name = sys.argv[2] if len(sys.argv) > 2 else "spark-on-eks-demo-data-ACCOUNT_ID"

    print(f"🔄 테스트 로그 생성 시작 (레코드 수: {num_records})")

    # 데이터 생성
    df = generate_web_logs(num_records)

    # 로컬 저장
    local_file = f"test_web_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(local_file, index=False)
    print(f"📁 로컬 파일 저장: {local_file}")

    # S3 업로드 (버킷이 제공된 경우)
    if len(sys.argv) > 2:
        try:
            s3_path = upload_to_s3(df, bucket_name)
            print(f"🎉 테스트 데이터 준비 완료!")
            print(f"   로컬: {local_file}")
            print(f"   S3: {s3_path}")
        except Exception as e:
            print(f"⚠️  S3 업로드 실패: {e}")
            print(f"로컬 파일은 생성되었습니다: {local_file}")
    else:
        print(f"📝 사용법: python3 {sys.argv[0]} [레코드수] [S3버킷명]")
        print(f"로컬 파일이 생성되었습니다: {local_file}")
EOF

    # Spark 로그 분석 스크립트
    cat > log_analysis_spark.py << 'EOF'
#!/usr/bin/env python3
"""
EMR on EKS에서 실행될 Spark 로그 분석 스크립트
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, count_distinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    window, to_timestamp, when, desc, hour, date_format, regexp_extract
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import sys

def create_spark_session():
    """Spark Session 생성"""
    spark = SparkSession.builder \
        .appName("WebLogAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    print("✅ Spark Session 생성 완료")
    return spark

def load_data(spark, input_path):
    """웹 로그 데이터 로드"""
    print(f"📂 데이터 로드 시작: {input_path}")

    # 스키마 정의
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("bytes_sent", IntegerType(), True),
        StructField("session_id", StringType(), True)
    ])

    # CSV 파일 읽기
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(input_path)

    # 타임스탬프 변환
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # 데이터 품질 확인
    total_records = df.count()
    valid_records = df.filter(col("timestamp").isNotNull()).count()

    print(f"📊 데이터 로드 완료")
    print(f"   총 레코드: {total_records:,}")
    print(f"   유효 레코드: {valid_records:,}")

    if valid_records == 0:
        raise ValueError("유효한 데이터가 없습니다!")

    # 데이터 샘플 출력
    print("📋 데이터 샘플:")
    df.show(5, truncate=False)

    return df.filter(col("timestamp").isNotNull())

def analyze_traffic_patterns(df):
    """트래픽 패턴 분석"""
    print("📈 시간대별 트래픽 패턴 분석 중...")

    # 시간대별 요청 수
    hourly_traffic = df.withColumn("hour", hour("timestamp")) \
        .groupBy("hour") \
        .agg(
            count("*").alias("request_count"),
            avg("response_time_ms").alias("avg_response_time"),
            spark_sum("bytes_sent").alias("total_bytes")
        ) \
        .orderBy("hour")

    print("⏰ 시간대별 트래픽:")
    hourly_traffic.show(24)

    return hourly_traffic

def analyze_status_codes(df):
    """HTTP 상태 코드 분석"""
    print("🔍 HTTP 상태 코드 분석 중...")

    status_analysis = df.groupBy("status_code") \
        .agg(
            count("*").alias("count"),
            (count("*") * 100.0 / df.count()).alias("percentage"),
            avg("response_time_ms").alias("avg_response_time")
        ) \
        .orderBy(desc("count"))

    print("📊 상태 코드별 통계:")
    status_analysis.show()

    return status_analysis

def analyze_endpoints(df):
    """API 엔드포인트 분석"""
    print("🛤️  API 엔드포인트 분석 중...")

    endpoint_analysis = df.groupBy("url", "method") \
        .agg(
            count("*").alias("hit_count"),
            avg("response_time_ms").alias("avg_response_time"),
            spark_max("response_time_ms").alias("max_response_time"),
            count(when(col("status_code") >= 400, 1)).alias("error_count")
        ) \
        .withColumn("error_rate", col("error_count") * 100.0 / col("hit_count")) \
        .orderBy(desc("hit_count"))

    print("🎯 엔드포인트별 통계:")
    endpoint_analysis.show(10, truncate=False)

    return endpoint_analysis

def analyze_performance(df):
    """성능 분석"""
    print("⚡ 성능 메트릭 분석 중...")

    # 응답 시간 분포
    performance_stats = df.agg(
        avg("response_time_ms").alias("avg_response_time"),
        spark_min("response_time_ms").alias("min_response_time"),
        spark_max("response_time_ms").alias("max_response_time"),
        count("*").alias("total_requests")
    )

    print("📏 응답 시간 통계:")
    performance_stats.show()

    # 느린 요청 분석 (95th percentile)
    slow_requests = df.filter(col("response_time_ms") > 1000) \
        .select("timestamp", "method", "url", "status_code", "response_time_ms") \
        .orderBy(desc("response_time_ms")) \
        .limit(10)

    print("🐌 느린 요청 TOP 10:")
    slow_requests.show(truncate=False)

    return performance_stats, slow_requests

def save_results(df_dict, output_path):
    """분석 결과를 S3에 저장"""
    print(f"💾 분석 결과 저장 중: {output_path}")

    for name, df in df_dict.items():
        result_path = f"{output_path}/{name}"
        print(f"   📁 {name} → {result_path}")

        # 작은 데이터는 단일 파티션으로 저장
        if df.count() < 1000:
            df = df.coalesce(1)

        # Parquet 형식으로 저장
        df.write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(result_path)

    print("✅ 모든 결과 저장 완료")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Spark 웹 로그 분석")
    parser.add_argument("--input", required=True, help="입력 데이터 S3 경로")
    parser.add_argument("--output", required=True, help="출력 결과 S3 경로")

    args = parser.parse_args()

    print("🚀 Spark 웹 로그 분석 시작")
    print(f"   입력: {args.input}")
    print(f"   출력: {args.output}")
    print("=" * 60)

    # Spark Session 생성
    spark = create_spark_session()

    try:
        # 데이터 로드
        df = load_data(spark, args.input)

        # 각종 분석 수행
        hourly_traffic = analyze_traffic_patterns(df)
        status_analysis = analyze_status_codes(df)
        endpoint_analysis = analyze_endpoints(df)
        performance_stats, slow_requests = analyze_performance(df)

        # 전체 요약 통계
        print("📋 전체 요약:")
        summary = df.agg(
            count("*").alias("total_requests"),
            count_distinct("ip_address").alias("unique_ips"),
            count_distinct("session_id").alias("unique_sessions"),
            avg("response_time_ms").alias("avg_response_time"),
            spark_sum("bytes_sent").alias("total_bytes_sent")
        )
        summary.show()

        # 결과 저장
        results = {
            "hourly_traffic": hourly_traffic,
            "status_analysis": status_analysis,
            "endpoint_analysis": endpoint_analysis,
            "performance_stats": performance_stats,
            "slow_requests": slow_requests,
            "summary": summary
        }

        save_results(results, args.output)

        print("🎉 웹 로그 분석 완료!")

    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
EOF

    # S3에 업로드
    aws s3 cp generate_test_logs.py s3://$DATA_BUCKET/scripts/
    aws s3 cp log_analysis_spark.py s3://$DATA_BUCKET/scripts/

    log_success "샘플 파일 생성 및 업로드 완료"
    echo "   - 테스트 데이터 생성기: s3://$DATA_BUCKET/scripts/generate_test_logs.py"
    echo "   - Spark 분석 스크립트: s3://$DATA_BUCKET/scripts/log_analysis_spark.py"
}

# EMR on EKS 작업 제출 스크립트 생성
create_job_submission_script() {
    log_info "EMR 작업 제출 스크립트 생성 중..."

    cat > submit_spark_job.py << EOF
#!/usr/bin/env python3
"""
EMR on EKS Spark 작업 제출 스크립트
"""
import boto3
import json
import time
import sys
from datetime import datetime

# 환경 설정
PROJECT_NAME = "$PROJECT_NAME"
REGION = "$REGION"
DATA_BUCKET = "$DATA_BUCKET"
LOGS_BUCKET = "$LOGS_BUCKET"
EMR_ROLE_ARN = "$EMR_ROLE_ARN"

def get_virtual_cluster_id():
    """Virtual Cluster ID 찾기"""
    emr_client = boto3.client('emr-containers', region_name=REGION)

    try:
        response = emr_client.list_virtual_clusters()
        clusters = [vc for vc in response['virtualClusters']
                   if vc['name'] == f'{PROJECT_NAME}-virtual-cluster' and vc['state'] == 'RUNNING']

        if clusters:
            return clusters[0]['id']
        else:
            print("❌ 실행 중인 Virtual Cluster를 찾을 수 없습니다.")
            print("먼저 다음 명령을 실행하세요: ./setup-emr-virtual-cluster.sh")
            return None
    except Exception as e:
        print(f"❌ Virtual Cluster 확인 실패: {e}")
        return None

def submit_spark_job(virtual_cluster_id, input_path, output_path):
    """Spark 작업 제출"""
    emr_client = boto3.client('emr-containers', region_name=REGION)

    job_name = f"log-analysis-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    job_config = {
        "name": job_name,
        "virtualClusterId": virtual_cluster_id,
        "executionRoleArn": EMR_ROLE_ARN,
        "releaseLabel": "emr-6.15.0-latest",
        "jobDriver": {
            "sparkSubmitJobDriver": {
                "entryPoint": f"s3://{DATA_BUCKET}/scripts/log_analysis_spark.py",
                "entryPointArguments": [
                    "--input", input_path,
                    "--output", output_path
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.instances=2 "
                    "--conf spark.executor.memory=2g "
                    "--conf spark.executor.cores=1 "
                    "--conf spark.driver.memory=2g "
                    "--conf spark.sql.adaptive.enabled=true "
                    "--conf spark.sql.adaptive.coalescePartitions.enabled=true "
                    "--conf spark.kubernetes.executor.deleteOnTermination=true "
                    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
            }
        },
        "configurationOverrides": {
            "applicationConfiguration": [
                {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true"
                    }
                }
            ],
            "monitoringConfiguration": {
                "cloudWatchMonitoringConfiguration": {
                    "logGroupName": f"/aws/emr-containers/{PROJECT_NAME}",
                    "logStreamNamePrefix": "spark-job"
                },
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{LOGS_BUCKET}/spark-logs/"
                }
            }
        }
    }

    print(f"🚀 Spark 작업 제출 중: {job_name}")
    print(f"   입력: {input_path}")
    print(f"   출력: {output_path}")

    try:
        response = emr_client.start_job_run(**job_config)
        job_id = response['id']

        print(f"✅ 작업 제출 성공!")
        print(f"   작업 ID: {job_id}")
        print(f"   Virtual Cluster: {virtual_cluster_id}")

        return job_id

    except Exception as e:
        print(f"❌ 작업 제출 실패: {e}")
        return None

def monitor_job(job_id, virtual_cluster_id):
    """작업 상태 모니터링"""
    emr_client = boto3.client('emr-containers', region_name=REGION)

    print(f"⏳ 작업 상태 모니터링 시작...")
    print("   상태를 확인하려면 Ctrl+C로 중단할 수 있습니다.")

    try:
        while True:
            response = emr_client.describe_job_run(
                virtualClusterId=virtual_cluster_id,
                id=job_id
            )

            job_run = response['jobRun']
            state = job_run['state']

            print(f"   📊 상태: {state}")

            if state == 'COMPLETED':
                print("🎉 작업 완료!")
                return True
            elif state in ['FAILED', 'CANCELLED']:
                print(f"❌ 작업 실패: {state}")
                if 'stateDetails' in job_run:
                    print(f"   세부사항: {job_run['stateDetails']}")
                return False

            time.sleep(30)  # 30초마다 상태 확인

    except KeyboardInterrupt:
        print("\\n⏸️  모니터링을 중단했습니다.")
        print(f"작업은 계속 실행 중입니다. 상태 확인:")
        print(f"aws emr-containers describe-job-run --virtual-cluster-id {virtual_cluster_id} --id {job_id}")
        return None

def main():
    print("🔥 EMR on EKS Spark 작업 제출기")
    print("=" * 50)

    # Virtual Cluster 확인
    virtual_cluster_id = get_virtual_cluster_id()
    if not virtual_cluster_id:
        sys.exit(1)

    # 입력 데이터 경로 (기본값 또는 인자에서 가져오기)
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = f"s3://{DATA_BUCKET}/input/"

    # 출력 경로
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f"s3://{DATA_BUCKET}/output/analysis_{timestamp}"

    print(f"📋 작업 설정:")
    print(f"   Virtual Cluster: {virtual_cluster_id}")
    print(f"   입력 데이터: {input_path}")
    print(f"   출력 경로: {output_path}")
    print()

    # 작업 제출
    job_id = submit_spark_job(virtual_cluster_id, input_path, output_path)

    if job_id:
        print()
        print("다음 명령으로 수동 모니터링 가능:")
        print(f"aws emr-containers describe-job-run --virtual-cluster-id {virtual_cluster_id} --id {job_id}")
        print()

        # 모니터링 여부 확인
        monitor_choice = input("작업 상태를 실시간으로 모니터링하시겠습니까? (y/N): ")
        if monitor_choice.lower() in ['y', 'yes']:
            monitor_job(job_id, virtual_cluster_id)
        else:
            print("모니터링을 건너뜁니다. 나중에 위 명령으로 상태를 확인하세요.")

if __name__ == "__main__":
    main()
EOF

    chmod +x submit_spark_job.py
    log_success "EMR 작업 제출 스크립트 생성 완료"
}

# 설정 파일 및 도움말 생성
create_configuration_files() {
    log_info "설정 파일 및 도움말 생성 중..."

    # 환경 변수 파일
    cat > .env << EOF
# EMR Spark on EKS 환경 변수 (MWAA 제외)
PROJECT_NAME=$PROJECT_NAME
REGION=$REGION
DATA_BUCKET=$DATA_BUCKET
LOGS_BUCKET=$LOGS_BUCKET
EMR_ROLE_ARN=$EMR_ROLE_ARN
CLUSTER_NAME=$CLUSTER_NAME
EOF

    # 테스트 스크립트
    cat > test_deployment.py << EOF
#!/usr/bin/env python3
"""
간단한 배포 테스트 스크립트
"""
import boto3

def test_s3_buckets():
    """S3 버킷 접근 테스트"""
    print("🪣 S3 버킷 테스트")
    s3_client = boto3.client('s3', region_name='$REGION')

    buckets = ['$DATA_BUCKET', '$LOGS_BUCKET']

    for bucket in buckets:
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"   ✅ {bucket}")
        except Exception as e:
            print(f"   ❌ {bucket}: {e}")

def test_eks_cluster():
    """EKS 클러스터 상태 확인"""
    print("🚢 EKS 클러스터 테스트")
    import subprocess

    try:
        result = subprocess.run(['kubectl', 'get', 'nodes'],
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✅ EKS 클러스터 접근 가능")
            node_count = len([line for line in result.stdout.split('\\n')
                            if 'Ready' in line])
            print(f"   📊 활성 노드 수: {node_count}")
        else:
            print("   ❌ EKS 클러스터 접근 실패")
    except FileNotFoundError:
        print("   ⚠️  kubectl이 설치되지 않음")

def test_emr_virtual_cluster():
    """EMR Virtual Cluster 확인"""
    print("⚡ EMR Virtual Cluster 테스트")
    emr_client = boto3.client('emr-containers', region_name='$REGION')

    try:
        response = emr_client.list_virtual_clusters()
        clusters = [vc for vc in response['virtualClusters']
                   if vc['name'] == '${PROJECT_NAME}-virtual-cluster']

        if clusters:
            cluster = clusters[0]
            print(f"   ✅ Virtual Cluster 상태: {cluster['state']}")
            print(f"   📋 ID: {cluster['id']}")
        else:
            print("   ⚠️  Virtual Cluster를 찾을 수 없습니다")
            print("   💡 ./setup-emr-virtual-cluster.sh 실행 필요")
    except Exception as e:
        print(f"   ❌ Virtual Cluster 확인 실패: {e}")

if __name__ == "__main__":
    print("🧪 EMR Spark on EKS 배포 테스트")
    print("=" * 50)
    test_s3_buckets()
    print()
    test_eks_cluster()
    print()
    test_emr_virtual_cluster()
    print()
    print("✅ 테스트 완료")
EOF

    chmod +x test_deployment.py

    # 사용법 가이드
    cat > QUICK_START.md << 'EOF'
# 🚀 EMR Spark on EKS 빠른 시작 가이드

## 1. 인프라 배포
```bash
./simple_deploy.sh
```

## 2. EKS 및 EMR Virtual Cluster 설정
```bash
./setup-emr-virtual-cluster.sh
```

## 3. 테스트 데이터 생성
```bash
# 10,000개 로그 레코드 생성 및 S3 업로드
python3 generate_test_logs.py 10000 YOUR_DATA_BUCKET_NAME
```

## 4. Spark 작업 실행
```bash
# 로그 분석 작업 제출
python3 submit_spark_job.py
```

## 5. 결과 확인
```bash
# S3에서 결과 확인
aws s3 ls s3://YOUR_DATA_BUCKET/output/ --recursive
```

## 💰 비용 절약
```bash
# 실습 완료 후 리소스 정리
./cost_optimization.sh
```

## 🔍 문제 해결
```bash
# 배포 테스트
python3 test_deployment.py

# 클러스터 상태 확인
kubectl get nodes
kubectl get pods -n emr

# Virtual Cluster 상태
aws emr-containers list-virtual-clusters
```
EOF

    log_success "설정 파일 생성 완료"
}

# 완료 메시지
show_completion_message() {
    log_success "🎉 간단한 EMR Spark on EKS 배포 완료!"
    echo "============================================"

    echo ""
    echo "📋 다음 단계:"
    echo "1. EKS 및 EMR Virtual Cluster 설정:"
    echo "   ./setup-emr-virtual-cluster.sh"
    echo ""
    echo "2. 테스트 데이터 생성:"
    echo "   python3 generate_test_logs.py 10000 $DATA_BUCKET"
    echo ""
    echo "3. Spark 작업 실행:"
    echo "   python3 submit_spark_job.py"
    echo ""
    echo "4. 배포 테스트:"
    echo "   python3 test_deployment.py"
    echo ""
    echo "📚 자세한 사용법: cat QUICK_START.md"
    echo ""
    echo "💰 예상 시간당 비용 (MWAA 제외): ~$0.19 (~₩250)"
    echo "   - EKS 제어 평면: $0.10"
    echo "   - EKS 노드 (t3.medium spot x2): ~$0.03"
    echo "   - NAT Gateway: $0.059"
    echo "   - 기타 (S3, CloudWatch): ~$0.001"
    echo ""
    echo "⚠️  실습 완료 후 리소스 정리: ./cost_optimization.sh"
}

# 메인 실행 함수
main() {
    echo "간단한 EMR Spark on EKS 배포를 시작합니다..."
    echo "MWAA 없이 직접 Spark 작업을 제출하는 방식입니다."
    echo ""

    read -p "계속 진행하시겠습니까? (y/N): " confirm

    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        echo "배포를 취소했습니다."
        exit 0
    fi

    # 단계별 실행
    check_prerequisites
    validate_template
    deploy_stack
    collect_outputs
    create_sample_files
    create_job_submission_script
    create_configuration_files
    show_completion_message
}

# 스크립트 실행
main "$@"