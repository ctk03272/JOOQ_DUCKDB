# DuckDB + jOOQ 분석 실행 기반

분석 클래스는 `DuckDbAnalysis<P>.query(parameters)`에 jOOQ SELECT만 작성한다. 공통 실행기가 연결·고유 스키마·입력 적재·트랜잭션·분석 실행·출력·정리를 담당한다. 사용량과 가격 분석은 독립적으로 실행되며 결과를 결합하지 않는다.

**입력 파일 주소는 요청에서 받고, 출력 주소는 사용하는 개발자가 호출할 때 `AnalysisRequest.output`으로 전달한다.** 운영 입력·출력 계약은 S3 주소 문자열이다. FileLocation과 로컬 파일 분기는 없다. S3 입력은 DuckDB의 `read_csv`·`read_parquet`, S3 출력은 DuckDB의 `COPY`를 직접 사용한다. JVM 전체 파일 다운로드·업로드나 AWS Java SDK 의존성은 추가하지 않았다. 입력은 파일별로 CSV 또는 Parquet를 명시하며 출력은 CSV다.

## 호출자가 입출력 주소 전달

```kotlin
val result = pipeline.process(
    AnalysisRequest(
        analysisId = MonthlyAnalyses.PRICE,
        parameters = Unit,
        requestId = "message-123", // 생략 시 UUID
        output = "s3://my-bucket/project-a/prices/result.csv",
        inputs = mapOf(
            PRICES to InputFile(request.priceS3Uri, InputFormat.PARQUET),
            READINGS to InputFile(request.readingsS3Uri, InputFormat.CSV),
        ),
    ),
)
println(result.output) // s3:// 결과 주소
```

`PRICES`, `READINGS`는 codegen 테이블 객체다. 요청은 파일 이름 규칙이나 입력 디렉터리에 의존하지 않는다. 주소 형식·파일 존재 여부·요청 ID·설정 매핑을 사전 검사하지 않는다. 주소 문자열은 바인드 파라미터로 DuckDB에 전달하고 실제 DB/스토리지 오류를 그대로 전파한다. 분석 ID나 필요한 입력이 없으면 일반 컬렉션 조회 오류가 발생한다.

HTTP/SQS 수신기를 새로 구현한 것은 아니며, 수신기에서 위 API로 요청을 전달한다. 출력 주소는 호출 코드가 정해 요청 객체에 넣는다. 외부 SQS 메시지에 출력 주소 필드를 추가해야 한다는 뜻은 아니다.

## 개발자가 결정하는 Kotlin 설정

`configuration/AnalysisConfiguration.kt`에 분석·입출력 테이블을 설정한다. 저장 주소는 분석 정의에 포함하지 않는다.

SQL 파일은 도메인별로 나누고 `application.yml`의 `analysis.schema-location`에 검색 패턴을 지정한다. 검색 패턴에는 `classpath:`를 붙이지 않는다. 개별 파일 목록이나 도메인 선택 설정은 없다.

```yaml
analysis:
  schema-location: db/**/*.sql
```

`measurement.sql`은 systems·readings·monthly_usage, `pricing.sql`은 prices·monthly_price를 정의한다. 파일 경계는 작성·관리 단위일 뿐이며 DB 논리 스키마는 기존 pipeline을 유지한다. 분석은 도메인을 선택하지 않는다.

`TableDefinitions`가 모든 SQL 파일을 한 번 읽고 jOOQ 파서로 CREATE TABLE 문을 찾아 (DB 스키마 이름, 테이블 이름)별로 보관한다. 파일명을 테이블명에서 추측하지 않는다. 각 작업은 `requiredInputs + optionalInputs + outputTable`에 명시된 테이블의 생성문만 실행한다. 가격 분석은 두 파일에 걸쳐 있는 readings·prices·monthly_price만 생성한다. 공유 상태에는 SQL 문자열만 보관하고 실행 쿼리 객체는 작업별로 생성한다.

빌드의 codegen도 `src/main/resources/db/**/*.sql`을 모두 읽어 빌드용 in-memory DB에 적용한다. Gradle이 YAML을 파싱하지 않고 별도 빌드 의존성도 추가하지 않는다. 생성된 코드는 기존 `src/main/kotlin` 위치에 유지한다.

현재 등록 형식은 CREATE TABLE 안에 컬럼과 제약조건을 정의하는 DDL이다. 별도의 ALTER TABLE·인덱스·뷰·시드 데이터 실행 기능은 포함하지 않는다. 참조 제약조건이 있다면 선택한 테이블 목록에 참조 대상도 포함하고 생성 순서를 맞춰야 한다.

```kotlin
@Bean
fun monthlyPrice() = ConfiguredAnalysis(
    id = MonthlyAnalyses.PRICE,
    schema = Pipeline.PIPELINE,
    query = MonthlyPriceAnalysis(),
    requiredInputs = listOf(PRICES, READINGS),
    outputTable = MONTHLY_PRICE,
)
```

출력 주소는 호출자가 지정한 값을 그대로 사용한다. 공통 실행기가 버킷·폴더·파일명·요청 ID를 덧붙이지 않는다. 같은 분석도 호출마다 다른 경로에 저장할 수 있다.

분석은 객체를 직접 참조하고, 테이블·컬럼은 codegen 객체를 참조한다. 공통 출력 단계는 ORDER BY를 추가하지 않는다. 분석 작성자는 쿼리에 필요한 ORDER BY를 직접 작성할 수 있다. DuckDB의 기본 preserve_insertion_order=true에서 정렬된 결과를 일반 INSERT로 적재하고 단일 테이블 SELECT와 COPY로 내보내는 현재 경로는 그 순서를 보존한다. JOIN·GROUP BY 자체는 순서를 보장하지 않으므로 정렬이 필요한 분석에서 ORDER BY를 작성한다. 같은 정렬 키를 가진 행끼리의 순서까지 정하려면 추가 정렬 키가 필요하다. 별도 입력·업무 검증 계층은 없다. 가격 기간 중첩·누락·참조 오류를 사전 검사하지 않고 분석 쿼리를 실행한다. SQL 값의 파라미터 바인딩과 실패 시 자원 정리는 유지하며 DuckDB 및 S3 오류는 그대로 전파한다. 직접 추가한 애플리케이션 완료 로그와 테스트 출력 로그도 제거했다.

## 호출별 분석 파라미터

`AnalysisId<P>`, `ConfiguredAnalysis<P>`, `DuckDbAnalysis<P>`, `AnalysisRequest<P>`가 같은 파라미터 타입을 사용한다. `PipelineService.process`와 공통 실행기는 이 타입을 유지해 요청의 `parameters`를 `query(parameters)`에 전달한다. 추가 인자가 없는 월별 분석은 `Unit`을 사용한다.

분석 ID는 한 번 선언한 객체를 등록과 호출에서 함께 참조한다. 기존 월별 분석은 `MonthlyAnalyses.USAGE`와 `MonthlyAnalyses.PRICE`다. 같은 문자열로 `AnalysisId`를 다시 생성해도 등록된 ID가 되지 않는다. 문자열 이름만으로 다른 파라미터 타입의 분석에 연결되지 않게 하기 위한 계약이다. ID는 내부 Kotlin API이며 외부 메시지 형식을 정의하지 않는다.

Capacity 분석은 다음 호출별 파라미터 타입을 사용한다. 실제 구현은 `analysis/capacity/`에 있다.

```kotlin
data class CapacityParameters(
    val rcList: List<String>,
    val xDockList: List<String>,
    val picoRcList: List<String>,
    val defaultEarlyShufflingRatio: Double,
    val isUnLimitCapacity: Boolean,
    val useEarlyShufflingForPicoRC: Boolean,
)

// 등록된 ID는 CapacityAnalyses.CAPACITY를 참조한다.
// 같은 이름으로 AnalysisId를 새로 만들지 않는다.
```

예를 들어 `MonthlyAnalyses.PRICE`에 `parameters = "wrong"`을 전달하면 타입 불일치로 컴파일되지 않는다. 공통 Registry는 서로 다른 타입의 분석들을 저장하므로 조회 한 곳에서만 제네릭 캐스트를 사용한다. 동일한 ID 객체의 타입과 분석 타입이 등록 시 연결되어 있으며, 문자열을 비교해 타입을 추정하지 않는다.

공유 분석 객체에 파라미터를 저장하지 않는다. 각 호출은 자신의 파라미터로 새 쿼리를 만든다. 파라미터는 임의의 Kotlin 타입이므로 공통 실행기가 깊은 복사를 하지 않는다. 호출자는 실행 중 파라미터나 내부 컬렉션을 변경하지 않고, 가변 목록에서 값을 가져온다면 요청을 만들 때 `toList()` 등으로 별도 값을 준비한다.

## Capacity DataFrame 로직 이식

`CapacityAnalysis`는 제공된 `IpsModelPreProcessCapacityCalculator` 사진의 계산 순서를 jOOQ SELECT로 구성한다. 입력과 출력 DDL은 `db/capacity.sql`에 있고 codegen을 통해 생성한다. 별도 DataFrame 의존성이나 분석별 실행기는 추가하지 않았다.

```kotlin
val result = pipeline.process(
    AnalysisRequest(
        analysisId = CapacityAnalyses.CAPACITY,
        inputs = mapOf(
            CAPACITY to InputFile(capacityS3Uri, InputFormat.CSV),
            PURCHASE_ORDER to InputFile(purchaseOrderS3Uri, InputFormat.CSV),
            FC_SHUTDOWN to InputFile(shutdownS3Uri, InputFormat.CSV),
            // 설정 파일이 없으면 이 항목을 생략한다.
            FC_EARLY_SHUFFLING_CONFIG to InputFile(filteredConfigS3Uri, InputFormat.CSV),
        ),
        output = outputS3Uri,
        parameters = CapacityParameters(
            rcList = rcList,
            xDockList = xDockList,
            picoRcList = picoRcList,
            defaultEarlyShufflingRatio = defaultEarlyShufflingRatio,
            isUnLimitCapacity = isUnLimitCapacity,
            useEarlyShufflingForPicoRC = useEarlyShufflingForPicoRC,
        ),
    ),
)
```

- `capacity`, `purchase_order`, `fc_shutdown`은 필수 입력이다. 데이터가 없으면 헤더만 있는 파일을 전달할 수 있다. `fc_early_shuffling_config`만 선택 입력이며 호출자가 이미 groupName으로 필터링한 파일을 전달한다. 분석에서 groupName을 다시 필터링하지 않는다.
- 공통 `ConfiguredAnalysis.optionalInputs`에 선언한 테이블은 파일 생략 시 빈 테이블로 만든다. 파일 주소를 전달했는데 읽기 실패가 발생하면 오류를 전파한다. 필수 입력 누락을 빈 입력으로 바꾸지 않는다.
- `purchase_order.date`와 `shutdownDt`는 `-`를 제거해 정수 날짜로 비교한다. 날짜는 기존 입력 계약인 `yyyy-MM-dd` 또는 `yyyyMMdd`를 대상으로 한다. 날짜·센터·유형별 `quantity`와 `modelAllocatedQuantity`를 집계한다. `confirmedQuantity`를 합산하지 않는다.
- 원래 용량을 보관한 뒤 발주량 차감과 예약 용량의 세 분기를 적용한다. 무제한 모드는 capacity와 revised_capacity를 Int.MAX_VALUE, reserved_capacity를 0으로 만든다. 그 다음 휴무일을 적용하므로 무제한 모드라도 휴무일에는 세 용량이 모두 0이다.
- 비율은 센터·유형별 설정 → 센터 공통 설정 → RC/XDock/PicoRC 기본 규칙 순으로 선택한다. 0.0은 유효한 설정값이며 null일 때만 다음 값으로 넘어간다. 무제한 모드의 비율은 0.0이다.
- 설정의 중복 키는 첫 입력 행을 유지한다. 적재된 설정 테이블의 rowid로 선택 순서를 명시하며, 첫 행의 비율이 null이어도 다음 중복 행을 대신 선택하지 않는다. 휴무일 입력의 중복 행은 기존 LEFT JOIN처럼 결과에 남는다.
- 예약량은 `min(revised_capacity, (ratio * old_capacity).toInt())`다. `CapacityNumbers`는 Kotlin Int의 오버플로, Double.toInt의 절삭·범위 제한·NaN 처리 의미를 SQL 식으로 유지한다. 발주 수량의 null은 원래 Number.toInt처럼 계산 실패로 처리하며 SUM에서 조용히 제외하지 않는다.
- 결과 `capacity_result`의 20개 컬럼과 컬럼 순서는 사진의 OUTPUT_COLUMNS와 같다. 테스트는 행 내용과 중복을 비교하며 최종 CSV의 행 순서 동일성을 보장하지 않는다. 공통 출력 정렬은 추가하지 않았다.

검증은 실제 Spring 설정과 주입된 PipelineService → 등록된 CapacityAnalysis → DuckDB 적재·계산·CSV 출력 경로로 수행했다. 테스트 전용 Kotlin 기준 구현은 사진의 계산식을 옮긴 것이며 원본 DataFrame 라이브러리를 실행한 비교가 아니다. 9개 테스트에서 계산 분기·null 용량·null 조인 키·중복·설정 생략/빈 파일·읽기 실패·숫자 변환·CSV/Parquet 혼합·100개 동시 요청의 결과와 자원 정리를 확인했다. 새 Capacity의 실제 S3 입출력과 대용량 성능은 이번에 검증하지 않았다.

이식 시 참고한 의미: [DataFrame distinctBy의 첫 행 선택](https://kotlin.github.io/dataframe/distinct.html), [DataFrame 조인의 키 비교 구현](https://github.com/Kotlin/dataframe/blob/master/core/src/main/kotlin/org/jetbrains/kotlinx/dataframe/impl/api/join.kt), [Kotlin Double.toInt](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-double/to-int.html), [DuckDB 입력 순서 보존](https://duckdb.org/docs/sql/dialect/order_preservation).

## AWS 설정과 인증

`configuration/AwsS3Configuration.kt`의 Kotlin Bean을 배포 환경에 맞게 설정한다.

```kotlin
@Bean
fun awsS3Settings() = AwsS3Settings(
    enabled = true,
    region = "ap-northeast-2",
    profile = "duckdb-poc",
    credentialChain = null,
)
```

- 실제 Spring Bean은 `enabled=true`, `region="ap-northeast-2"`, `profile="duckdb-poc"`다. Spring 컨텍스트를 시작하는 일반 테스트도 이 설정으로 AWS 초기화를 수행하므로 해당 로컬 프로필과 AWS 연결이 필요하다. 데이터 클래스 생성자의 기본값과 실제 Bean 설정을 구분한다.
- 활성화하면 공유 인스턴스 초기화 단계에서 `httpfs`, `aws` 확장을 설치·로드하고 `credential_chain` 기반 secret을 생성한다. 작업별로 반복 생성하지 않는다.
- 확장 설치 경로는 프로젝트의 `build/duckdb-extensions`다. 최초 설치에는 공식 확장 저장소 접근이 필요하다. 컨테이너에서는 같은 DuckDB 버전의 확장을 이 경로에 미리 준비하거나 설치 가능한 시작 환경을 제공한다.
- `region`, `profile`, `credentialChain`은 Kotlin에서 명시할 수 있다. null이면 DuckDB의 AWS credential chain 설정을 따른다. 예: 로컬 프로필은 `profile = "dev", credentialChain = "config"`로 선택할 수 있다. 환경·프로필·워크로드 역할 인증의 실제 가용성은 해당 배포 환경에서 검증해야 한다.
- Access key/secret을 코드에 저장하지 않는다. secret은 `PERSISTENT`로 생성하지 않는다. `REFRESH auto`를 사용한다.
- 리전·입출력 버킷 권한·IAM Role 설정은 실제 배포 대상에 맞게 준비해야 한다. S3 직접 쓰기는 multipart upload를 사용한다.

실제 AWS 인증 정보가 없는 상태에서 AWS 연결 성공을 검증했다고 주장하지 않는다. 참고: [DuckDB AWS 인증](https://duckdb.org/docs/2.0/core_extensions/aws), [S3 직접 입출력](https://duckdb.org/docs/2.0/core_extensions/httpfs/s3api).

## CSV 입력

`InputFile(address, format)`의 형식은 CSV 또는 PARQUET다. 호출 코드에서 파일마다 형식을 명시하며 확장자로 추측하지 않는다.

CSV는 쉼표 구분, 헤더 있음, UTF-8을 대상으로 한다. 헤더의 컬럼 이름으로 선택하므로 파일의 컬럼 순서에 의존하지 않는다. 입력값은 문자열로 읽고 선언된 대상 테이블 타입에 삽입하면서 변환한다. 샘플 데이터로 숫자·날짜 타입을 추론하지 않으며 타입 변환 실패는 DuckDB 오류로 전파된다. 전체 파일을 JVM 컬렉션에 올리지 않는다.

현재 요청은 테이블 하나당 InputFile 하나를 전달한다. 서로 다른 수십 개 테이블을 지정하는 데 고정 개수 제한은 없지만, 같은 테이블에 파일 목록을 전달하는 API는 포함하지 않는다.

## 애플리케이션 시작과 분석 호출

Java 21과 기존 고정 버전을 유지한다. `./gradlew build`로 빌드한다.

`main`은 Spring 컨텍스트를 시작한다. 기동 시 분석을 자동 실행하거나 입력·출력 명령줄 인자를 해석하지 않는다. `runApplication` 반환 직후 컨텍스트를 닫는 코드도 없다. 분석은 사용하는 코드에서 주입받은 `PipelineService.process(AnalysisRequest(...))`를 호출할 때 실행된다.

현재 HTTP 서버나 SQS 메시지 리스너는 구현되어 있지 않다. 따라서 단독 실행을 지속적인 메시지 소비자 동작으로 검증한 것은 아니다. 임의의 대기 루프나 keep-alive 설정은 추가하지 않는다. 분석 설정에는 고정 파일명이 없으며 로컬 fixture 조립은 src/test에만 있다.

## 분석 쿼리만 작성

```kotlin
interface DuckDbAnalysis<P> {
    fun query(parameters: P): Select<*>
}

class MonthlyPriceAnalysis : DuckDbAnalysis<Unit> {
    override fun query(parameters: Unit) = select(
        READINGS.SYSTEM_ID.`as`(MONTHLY_PRICE.SYSTEM_ID),
        year(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_YEAR),
        month(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_MONTH),
        sum(READINGS.POWER.mul(PRICES.VALUE))
            .`as`(MONTHLY_PRICE.PRICED_POWER_SUM),
    ).from(READINGS)
        .join(PRICES).on(PricePeriod.matchesReading())
        .groupBy(READINGS.SYSTEM_ID, year(READINGS.READ_ON), month(READINGS.READ_ON))
}
```

쿼리는 연결 없이 생성하고 공통 실행기가 작업별 연결로 실행한다. 분석 객체에 작업 상태나 실행 중인 가변 Query를 저장하지 않는다. 분석별 Factory, Workspace, PipelineDefinition, Service를 구현할 필요가 없다.

새 분석은 필요한 테이블을 해당 도메인 SQL에 정의하고 새 파일도 db/ 아래에 둔 뒤 codegen을 실행한 다음, `query(parameters)` 구현과 Kotlin 설정을 추가한다. SELECT와 대상 테이블의 컬럼 대응은 분석 코드가 정의한다. 공통 실행기에 별도의 컬럼 이름·순서 사전 검사는 없고 SQL 실행 오류는 DuckDB에서 전파된다.

## 계층과 수명

```text
application/           호출자가 전달한 요청의 실행 연결
configuration/         Kotlin 분석 설정·AWS 설정·DB 자원 설정
analysis/monthly/      분석 쿼리
framework/analysis/    분석 계약·설정 등록
framework/pipeline/    입력 요청·공통 실행기·결과
adapter/               read_csv·read_parquet·COPY
infrastructure/        연결·스키마·jOOQ 매핑·AWS 초기화
```

공유 in-memory 인스턴스 하나에서 작업마다 별도 연결·고유 스키마를 사용한다. 적재·분석 저장은 트랜잭션으로 실행하고 커밋 후 내보낸다. 성공·실패 모두 작업 스키마와 연결을 정리한다. 전체 작업 잠금이나 공유 현재 연결 필드는 없다.

S3 출력은 DuckDB가 최종 S3 객체에 직접 COPY한다. DB 트랜잭션이 S3까지 원자적으로 묶이는 것은 아니다. COPY가 실패하면 작업 실패를 반환하며 S3 성공으로 보고하지 않는다. 프로세스 강제 종료나 전송 실패에 따른 미완료 multipart upload의 정리는 버킷의 lifecycle 정책 등 운영 설정의 영역이다. 생성된 최종 출력 객체는 애플리케이션이 자동 삭제하지 않는다.

`DuckDbProperties`는 기본값 없이 Spring 설정을 바인딩한다. `src/main/resources/application.yml`의 `duckdb.memory-limit: 2GB`, `duckdb.threads: 4`가 실제 값을 공급한다. 별도 환경변수 입력을 전제로 하지 않는다. `duckdb.spill-directory`는 `${java.io.tmpdir}/duckdb-spill`로 설정하여 JVM 기본 임시 디렉터리 아래를 사용한다. 세 값 모두 YAML에서 공급하며 Kotlin 기본값은 없다. 테스트나 Gradle에서 누락된 값을 대신 채우지 않는다.

메모리 한도는 JVM 힙과 모든 native 할당의 총상한은 아니다. 지원 연산은 디스크 spill을 사용할 수 있다. codegen은 `src/main/kotlin/com/example/duckdbpoc/generated`에 생성하며 수동 편집하지 않는다.

## 검증

애플리케이션은 입출력 행 수와 단계별 실행 시간을 수집하지 않는다. `PipelineResult`에는 출력 주소만 담는다. 테스트의 CSV 행 수·내용 및 자원 개수 확인은 결과 검증용이며 운영 코드의 통계 수집이 아니다. 별도 동시 실행 테스트는 시간 측정·통계 보고서를 생성하지 않고 결과 일치, 작업 겹침, 자원 정리를 검증한다.

```bash
./gradlew test       # 실제 로컬 DuckDB 처리 + S3 SQL 어댑터 계약 테스트
./gradlew loadTest   # 로컬 사용량/가격 100개 혼합 동시 실행
./gradlew loadTest -PloadJobs=120
```

- 일반 테스트는 기존 로컬 fixture 통합 테스트 11개, 호출별 파라미터 통합 테스트 2개, Capacity 통합 테스트 9개, S3 SQL 어댑터 테스트 4개로 총 26개다. 입력 누락과 실제 DuckDB 파일 읽기 실패의 예외 전파·정리를 검증한다.
- 파라미터 통합 테스트는 실제 Spring 서비스·등록·실행기·DB 설정에 테스트용 분석 Bean만 추가한다. 서로 다른 목록·배수·플래그를 가진 100개 동시 요청의 CSV 내용, 다른 파라미터 타입의 분석 등록, 요청 재사용 시 값 변경, 동일 표시 이름을 가진 미등록 ID의 거부와 자원 정리를 검증한다. 이 테스트의 입출력은 로컬 fixture이며 실제 S3 입출력 검증이 아니다.
- 기존 계산·가격 경계·마이크로초·스키마/연결 정리 검증을 유지한다. 제거한 업무 검증의 예외를 기대하던 테스트 2개도 제거했다.
- S3 SQL 어댑터 테스트는 JDBC mock으로 주소 바인딩·오류 전파를 확인한다. 실제 AWS 권한·네트워크·multipart 성공 증거가 아니다.
- 별도 부하 테스트에서 로컬 사용량 50개·가격 50개 요청이 모두 성공했고 분석별 출력 일치와 스키마·연결 정리를 확인했다. S3 100개 요청의 성능 보장은 아니다. 보고서: `build/load-test-results/run-7fc60f55-32fd-4e3f-8d16-f1edfa452dd5/report.txt`.

실제 S3 입출력 검증은 별도 opt-in `s3Test`다. 일반 build/test/loadTest에는 이 테스트를 포함하지 않지만, Spring 컨텍스트를 사용하는 테스트의 AWS 초기화는 실제 Bean 설정을 따른다. 로컬 fixture 테스트는 계산 기능 검증이며 실제 S3 실행 경로의 증거로 사용하지 않는다.

```bash
# AWS 인증은 배포 환경/프로필에 준비한다. 아래 변수에는 주소만 설정한다.
export S3_TEST_PRICES_URI='s3://s3-stream-test-tony/prices.parquet'
export S3_TEST_READINGS_URI='s3://s3-stream-test-tony/readings.parquet'
# 출력 파일명은 예시다. 재실행 시 호출자가 새 주소를 지정할 수 있다.
export S3_TEST_PRICE_OUTPUT_URI='s3://s3-stream-test-tony/output/caller-a/prices.csv'
export S3_TEST_PRICE_ALTERNATE_OUTPUT_URI='s3://s3-stream-test-tony/output/caller-b/reports/prices.csv'
export S3_TEST_USAGE_OUTPUT_URI='s3://s3-stream-test-tony/output/caller-c/usage.csv'
export S3_TEST_SYSTEMS_URI='s3://s3-stream-test-tony/systems.parquet'
export S3_TEST_READINGS_CSV_URI='s3://s3-stream-test-tony/output/csv-fixtures/readings.csv'
export S3_TEST_CSV_OUTPUT_URI='s3://s3-stream-test-tony/output/csv-result/prices.csv'
./gradlew s3Test
```

이 테스트는 실제 Spring 컨텍스트에서 주입받은 `PipelineService`로 가격·사용량 분석을 각각 실행한다. AWS 설정, 분석 설정, 서비스와 실행기를 테스트에서 대체하지 않는다. 호출자의 입출력 주소를 환경변수로 받아 요청에 전달하며 누락 시 실패한다. 같은 가격 분석을 동일 요청 ID와 서로 다른 출력 주소로 두 번 실행하여 호출자가 지정한 경로를 그대로 사용하는지 확인한다. 기동 동작을 끄는 테스트 전용 설정 없이 컨텍스트를 시작하고 주입된 서비스에 요청을 전달한다. 요청의 출력 주소에 생성된 CSV를 다시 읽어 행 수와 작업 스키마 정리를 검증하고 결과 객체는 보존한다.

### 이전 실제 설정 검증 기록 (2026-11-03, 출력 주소를 요청으로 이동하기 전)

- `build s3Test` 성공: 기존 기능 테스트 13개, 실제 Spring Bean을 사용하는 S3 테스트 2개 통과. 실패·건너뜀 없음.
- 가격·사용량 분석 모두 입력 측정 데이터 151,879행 → 출력 54행. S3 CSV 재조회 및 작업 스키마 정리 확인.
- 가격 출력: `s3://s3-stream-test-tony/output/monthly-price/e7ff0d2c-3997-4954-9b4b-5c8e560e8135.csv`
- 사용량 출력: `s3://s3-stream-test-tony/output/monthly-usage/104e84b6-77ab-4ad4-bf96-c392ca5f0359.csv`
- 검증 범위는 현재 로컬 AWS 프로필과 예제 데이터다. S3 100개 동시 실행, 대용량 성능, 컨테이너 IAM Role은 이번 검증에 포함하지 않았다.

### 호출자 출력 주소 검증 (2026-11-03)

- 기본 테스트 13개 및 실제 Spring S3 테스트 2개 통과, 실패·건너뜀 없음.
- 같은 가격 분석과 같은 요청 ID에서 출력 주소만 바꿔 `output/caller-a/20261103-request-output/prices.csv`와 `output/caller-b/20261103-request-output/reports/prices.csv`에 각각 저장했다. 사용량은 `output/caller-c/20261103-request-output/usage.csv`에 저장했다. 모두 `s3-stream-test-tony` 버킷이며 151,879행 처리, CSV 54행 재조회, 작업 스키마 정리를 확인했다.
## 도메인 SQL 분리와 CSV 입력 검증 (2026-11-03)

- codegen을 실제 재실행하고 빌드 완료. JAR에 measurement.sql·pricing.sql이 포함되며 이전 schema.sql은 포함되지 않는다.
- 일반 테스트 15개 통과: 선택한 테이블만 생성, 도메인 파일을 넘나드는 가격 분석, CSV·Parquet 혼합 결과 일치, CSV 헤더 순서 차이, 소수·마이크로초 보존 등을 확인했다.
- 별도 100개 동시 작업 테스트 통과. 두 분석별 출력 일치와 작업 스키마·연결 정리를 확인했다.
- 실제 Spring Bean을 사용하는 S3 테스트 3개 통과. S3의 readings.parquet에서 CSV fixture를 `output/20261103-domain-sql/fixtures/readings.csv`에 생성한 후 실제 서비스로 CSV·Parquet 혼합 요청을 처리했다. 테스트 fixture 준비 외에 AWS 설정·스키마 설정·서비스를 대체하지 않았다.
- 검증은 현재 예제의 5개 테이블·2개 도메인 SQL을 사용했다. 수십 종류의 실제 입력 데이터와 대용량 CSV 성능을 실측한 결과는 아니다. 업무 로그·통계·사전 검증은 추가하지 않았다.

## 고정 버전

| 항목 | 버전 |
|---|---|
| Java | 21 |
| Spring Boot | 3.5.16 |
| Kotlin | 1.9.25 |
| jOOQ / codegen | 3.21.9 |
| DuckDB JDBC | 2.0.0.1 |
| Gradle Wrapper | 8.14.3 |

샌드박스에서 사용한 빌드 명령:

```bash
GRADLE_USER_HOME="$PWD/.gradle-user-home" ./gradlew build loadTest \
  --no-daemon --console=plain --no-watch-fs \
  -Pkotlin.compiler.execution.strategy=in-process
```
