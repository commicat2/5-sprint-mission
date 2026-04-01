# Discodeit

Discord 스타일의 메시징 플랫폼 백엔드. DDD 레이어드 아키텍처 기반의 이벤트 주도 설계.

## 기술 스택

### 백엔드

<div>
  <img src="https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Spring_Boot-3.4.5-6DB33F?logo=springboot&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Gradle-8.14-02303A?logo=gradle&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Spring_Security-6.x-6DB33F?logo=springsecurity&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Hibernate-6.x-59666C?logo=hibernate&logoColor=white" alt="badge">
</div>

### 데이터베이스 & 캐시

<div>
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Caffeine-L1_Cache-6DB33F" alt="badge">
</div>

### 메시징 & 스토리지

<div>
  <img src="https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Amazon_S3-569A31?logo=amazons3&logoColor=white" alt="badge">
</div>

### 모니터링 & 인프라

<div>
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Spring_Actuator-6DB33F?logo=spring&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Prometheus-E6522C?logo=prometheus&logoColor=white" alt="badge">
</div>

### 테스트

<div>
  <img src="https://img.shields.io/badge/JUnit_5-25A162?logo=junit5&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/Mockito-BDD-78A641" alt="badge">
  <img src="https://img.shields.io/badge/Testcontainers-2496ED?logo=docker&logoColor=white" alt="badge">
  <img src="https://img.shields.io/badge/JaCoCo-Coverage-C21325" alt="badge">
</div>

## 아키텍처

```
com.sprint.mission.discodeit/
├── domain/                    도메인 모듈 (수직 슬라이스)
│   ├── auth/                  인증, JWT, 감사 로깅
│   ├── user/                  사용자 관리, 프로필
│   ├── channel/               채널 (PUBLIC/PRIVATE)
│   ├── message/               메시지 및 파일 첨부
│   ├── binarycontent/         파일 저장소 메타데이터 및 상태 추적
│   ├── readstatus/            채널별 메시지 읽음 추적
│   └── notification/          사용자 알림
├── global/                    횡단 관심사 (공유 커널)
│   ├── config/                설정 (Security, JPA, Cache, Async, S3, ...)
│   ├── security/              JWT, 로그인 제한, 사용자 인증 정보
│   ├── cache/                 캐시 유틸리티
│   ├── error/                 ErrorCode, GlobalExceptionHandler
│   └── outbox/                Outbox 패턴 인터페이스
└── infrastructure/            어댑터
    ├── messaging/             Kafka 구독자, Outbox 릴레이
    └── storage/               S3 스토리지, 고아 파일 정리
```

각 도메인 모듈은 동일한 내부 구조를 따릅니다:
- `application/` — 서비스, 매퍼, 파사드
- `domain/` — 엔티티, 리포지토리, 도메인 이벤트, 예외
- `presentation/` — 컨트롤러, API 문서 인터페이스, DTO

## API 엔드포인트

| 메서드 | 엔드포인트 | 설명 |
|--------|----------|------|
| `POST` | `/api/auth/login` | 로그인 (폼) |
| `POST` | `/api/auth/refresh` | 액세스 토큰 갱신 |
| `POST` | `/api/auth/logout` | 로그아웃 |
| `PUT` | `/api/auth/role` | 사용자 권한 변경 (ADMIN 전용) |
| `GET` | `/api/auth/csrf-token` | CSRF 토큰 조회 |
| `POST` | `/api/users` | 사용자 생성 |
| `GET` | `/api/users` | 사용자 목록 조회 |
| `PATCH` | `/api/users/{id}` | 사용자 수정 |
| `DELETE` | `/api/users/{id}` | 사용자 삭제 |
| `POST` | `/api/channels/public` | 공개 채널 생성 |
| `POST` | `/api/channels/private` | 비공개 채널 생성 |
| `GET` | `/api/channels` | 채널 목록 조회 |
| `PATCH` | `/api/channels/{id}` | 채널 수정 |
| `DELETE` | `/api/channels/{id}` | 채널 삭제 |
| `POST` | `/api/messages` | 메시지 생성 (멀티파트) |
| `GET` | `/api/messages` | 메시지 목록 조회 (커서 페이지네이션) |
| `PATCH` | `/api/messages/{id}` | 메시지 수정 |
| `DELETE` | `/api/messages/{id}` | 메시지 삭제 |
| `GET` | `/api/binaryContents` | ID 목록으로 파일 조회 |
| `GET` | `/api/binaryContents/{id}` | 파일 메타데이터 조회 |
| `GET` | `/api/binaryContents/{id}/download` | 파일 다운로드 (S3 Presigned URL) |
| `POST` | `/api/readStatuses` | 읽음 상태 생성 |
| `GET` | `/api/readStatuses` | 읽음 상태 목록 조회 |
| `PATCH` | `/api/readStatuses/{id}` | 읽음 상태 수정 |
| `GET` | `/api/notifications` | 알림 목록 조회 |
| `DELETE` | `/api/notifications/{id}` | 알림 확인 처리 |

API 문서: `/docs/ui` (Swagger UI)

## ERD

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│      users       │     │     channels     │     │ binary_contents  │
├──────────────────┤     ├──────────────────┤     ├──────────────────┤
│ id          (PK) │     │ id          (PK) │     │ id          (PK) │
│ username  (UQ)   │     │ type             │     │ file_name        │
│ email     (UQ)   │     │ name             │     │ size             │
│ password         │     │ description      │     │ content_type     │
│ role             │     │ created_at       │     │ status           │
│ profile_id (FK)──┼─────┤ updated_at       │     │ created_at       │
│ created_at       │     └────────┬─────────┘     │ updated_at       │
│ updated_at       │              │               └────────┬─────────┘
└──────┬───────────┘              │                        │
       │                         │                        │
       │    ┌────────────────────┐│   ┌────────────────────┘
       │    │     messages       ││   │  message_attachments
       │    ├────────────────────┤│   ├──────────────────────┐
       │    │ id            (PK) ││   │ message_id      (PK) │
       ├───>│ author_id     (FK) ││   │ attachment_id   (PK) │
       │    │ channel_id    (FK)─┘│   │ order_index          │
       │    │ content            │<───┤                      │
       │    │ created_at         │    └──────────────────────┘
       │    │ updated_at         │
       │    └────────────────────┘
       │
       │    ┌────────────────────┐    ┌──────────────────────┐
       │    │   read_statuses    │    │    notifications     │
       │    ├────────────────────┤    ├──────────────────────┤
       │    │ id            (PK) │    │ id            (PK)   │
       │    │ user_id       (FK) │    │ receiver_id   (FK)   │
       ├───>│ channel_id    (FK) │    │ title                │
       │    │ last_read_at       │    │ content              │
       │    │ notification_enabled    │ checked              │
       │    │ created_at         │    │ created_at           │
       │    └────────────────────┘    └──────────────────────┘
       │
       │    ┌────────────────────┐    ┌──────────────────────┐
       │    │  auth_audit_logs   │    │    outbox_events     │
       │    ├────────────────────┤    ├──────────────────────┤
       └───>│ user_id            │    │ id            (PK)   │
            │ event_type         │    │ aggregate_type       │
            │ username           │    │ aggregate_id         │
            │ ip_address         │    │ topic                │
            │ user_agent         │    │ payload       (JSONB)│
            │ details            │    │ status               │
            │ created_at         │    │ published_at         │
            └────────────────────┘    │ created_at           │
                                      └──────────────────────┘
```

## 주요 기능

- **JWT 인증** — Access/Refresh 토큰, 시크릿 로테이션, 세션 관리
- **역할 기반 접근 제어** — 권한 계층 (ADMIN > CHANNEL_MANAGER > USER), 메서드 수준 보안
- **이벤트 주도 아키텍처** — Spring Events + Kafka + Outbox 패턴으로 안정적 이벤트 발행
- **이중 캐시** — Caffeine (로컬) / Redis (분산), 설정으로 전환 가능
- **S3 파일 스토리지** — Presigned URL 다운로드, 비동기 업로드 및 재시도, 고아 파일 정리
- **로그인 시도 제한** — 시도 횟수/윈도우/차단 시간 설정 가능, 인메모리 또는 Redis 기반
- **감사 로깅** — 인증 이벤트를 IP, User Agent, 상세 정보와 함께 기록
- **비동기 처리** — MDC/SecurityContext 전파를 지원하는 스레드 풀

## 시작하기

### 사전 요구사항

- Docker & Docker Compose

### 실행

```bash
# 클론 및 시작
git clone <repo-url> && cd discodeit

# 설정 (.env에서 JWT 시크릿, AWS 자격 증명 등 수정)
vi .env

# 전체 서비스 시작 (PostgreSQL, Redis, Kafka, App)
docker compose up -d --build

# 상태 확인
docker compose ps
docker compose logs -f app
```

앱은 `http://localhost:8080`에서 시작됩니다. Swagger UI: `http://localhost:8080/docs/ui`

### 로컬 개발 (Docker 없이)

```bash
# 사전 요구사항: PostgreSQL, Redis, Kafka가 로컬에서 실행 중이어야 합니다
# .env에 로컬 연결 정보를 설정하세요

./gradlew bootRun
```

### 테스트 실행

```bash
./gradlew test                                    # 전체 테스트
./gradlew test --tests UserServiceTest            # 특정 클래스
./gradlew jacocoTestReport                        # 커버리지 리포트
./gradlew checkstyle                              # 코드 스타일 검사
```

## 프로필

| 프로필 | DB | 포트 | 로깅 | 용도 |
|--------|-----|------|------|------|
| `local` | PostgreSQL (localhost) | 8080 | DEBUG | 개발 |
| `prod` | PostgreSQL (환경변수) | 80 | WARN/INFO | 운영 |
| `test` | H2 (인메모리) | — | DEBUG | 테스트 |
