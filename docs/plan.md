# stepcache 구현 계획

상태: 확정. 설계는 [design.md](design.md)에 있고, 이 문서는 만드는 순서와 확인 방법만 적는다.

## 1. 범위

- 이 저장소의 라이브러리만 다룬다. 사용처의 이주는 각 사용처 저장소의 계획이다.
- stepcache는 범용 라이브러리다. 특정 사용처의 구조에 맞춘 동작은 넣지 않는다.
- 0.1.0에 넣는 것: 단계·키·저장소·`run`, 동시성(`run_many`, `async_run`), 매니페스트와 stale 조회.
- 0.1.0에 넣지 않는 것(쓰는 곳이 생기면 만든다):
  - `import_records` (설계 7)
  - `explain`, 통계 (설계 4.6)
  - 정리 `cleanup` (설계 8)
  - 스키마 이주 명령 `stepcache migrate` (결정 16)
  - `PostgresStore` (결정 1)

## 2. 정한 것

### 고른 것

| 항목 | 정한 값 |
|---|---|
| 스키마 범위 | 처음부터 설계 전체의 테이블과 열을 만든다. 0.1.0이 쓰지 않는 열(`last_used_at`, `input_key`, `producer`)도 포함한다. 뒤에 기능을 넣을 때 이주가 없다. |
| `last_used_at` | 0.1.0부터 적중 때 갱신한다(키마다 하루 한 번까지). 정리는 크기와 마지막 사용 시각을 함께 보므로(설계 8) 처음부터 맞는 값이 있어야 한다. |
| 단계 버전 저장 | `versions` 테이블에 버전마다 한 번. 레코드와 매니페스트 항목은 `version_sha`만 가진다. |
| 매니페스트 항목 | `manifest_entries` 테이블. stale 조회를 SQL 한 번으로 한다. |
| 빈 DB | 열 때 스키마를 만든다. `schema_version`이 다르면 오류를 낸다. |
| `run`의 소스 파일이 없을 때 | `Step`을 만들 때 오류를 내고 `code=False`를 쓰라고 알려 준다. |
| CI | GitHub Actions, Linux만. M1의 PR에서 넣는다(4절). mac은 개발하면서 로컬에서 돌린다. |
| 작업 단위 | 마일스톤마다 브랜치와 PR 하나. "확인" 항목을 테스트로 먼저 쓴다. |

### 기본값

| 항목 | 정한 값 | 이유 |
|---|---|---|
| Python | 3.12 이상 | 사용처들의 최저 버전이다. |
| 의존성 | 없음 | `sqlite3`, `hashlib`, `fcntl`, `asyncio`로 충분하다. |
| 빌드·테스트 | hatchling, `src/stepcache`, pytest, uv | 순수 Python 패키지라 이것으로 충분하다. |
| 파일 구성 | `step.py`(Step, Version, Blob, Refused, 키), `store.py`(SqliteStore), `cache.py`(StepCache, 매니페스트) | 세 파일로 시작하고, 커지면 나눈다. |
| DB 연결 | 스레드마다 하나. `fork`한 자식은 새로 연다. | 한 `StepCache`를 여러 스레드가 같이 쓴다(설계 5.1). |
| 키의 입력 표기 | 입력마다 `['blob', sha]` 또는 `['json', 값]` | blob과 같은 모양의 JSON 값이 같은 키가 되지 않는다. |
| 대기 간격 | 0.5초 고정 | 단계가 초 단위라 더 촘촘할 필요가 없다. |
| `run_batch`가 없는 단계의 `run_many` | 미스마다 `run`을 차례로 부른다. | 단계 작성자가 두 함수를 다 쓰지 않아도 된다. |
| JSON으로 남기는 이력 | 그런 곳이 생기면 JSONL(한 줄에 한 건)을 먼저 고려한다. | 0.1.0에는 이력을 JSON으로 남기는 곳이 없다. |

## 3. 마일스톤

### M0. 공개 저장소의 첫 커밋

- README, LICENSE(Apache-2.0), 설계, 이 계획을 커밋 하나로 올린다.
- 사용처 이름, 내부 이슈 번호, 패키지 색인 주소, 운영 환경 구성은 저장소에 넣지 않는다. `.gitignore`에 든 `internal/` 폴더에 둔다.

### M1. 단계, 키, 저장소, `run`

만들 것:

- 패키지 뼈대: `pyproject.toml`(라이선스 Apache-2.0), `src/stepcache`, `tests`.
- CI 워크플로(4절).
- `Blob`, `Refused`, `Version.of`, `Version.files`, `Step`.
- 키: 정규화 JSON, 키 형식 `'stepcache/1'`, `input_key`.
- `SqliteStore`: 스키마 만들기와 판 확인, 레코드(`get`, `put_if_absent`, `delete`), blob(`put`, `get`, `has`, `touch`, `delete`), `versions`.
- `StepCache.run`, `invalidate`. 이 마일스톤에서는 실행 중 표시 없이 동작한다.

확인:

- 같은 입력으로 두 번 부르면 두 번째는 적중이고 단계는 한 번만 실행된다.
- 단계 이름, `code`, `files`, `tools`, `rev` 가운데 하나만 바꿔도 키가 바뀐다. 입력 사전의 순서와 파일 이름은 키에 영향이 없다.
- 앞 단계의 버전이 바뀌어도 출력 바이트가 같으면 뒤 단계는 적중한다(설계 3.4).
- 거절은 저장되어 다시 묻지 않는다. 예외는 저장되지 않고 다음에 다시 실행된다.
- 레코드는 있는데 blob이 없으면 미스다.
- JSON 출력이 4 KiB를 넘으면 오류다. blob은 256 KiB를 경계로 DB와 파일에 나뉘어 들어간다.
- `put_if_absent`는 이미 있는 레코드를 돌려준다.
- 무효화하면 다음 실행이 새로 계산한다.
- 빈 DB를 여러 프로세스가 동시에 처음 열어도 스키마는 한 번만 만들어진다. `schema_version`이 다른 DB는 열 때 오류다.
- NaN이 든 입력은 오류다. 소스 파일이 없는 `run`으로 `Step`을 만들면 오류다.
- 적중하면 `last_used_at`이 갱신되고, 같은 날 다시 적중하면 쓰지 않는다.

### M2. 동시성: 실행 중 표시, `run_many`, `async_run`

만들 것:

- `claims` 테이블과 프로세스 잠금 파일(설계 6.2). `fork`한 자식은 자기 잠금 파일을 만들고, 정상 종료 때 자기 파일을 지운다.
- 내부 두 동작 "조회하고 표시하기", "결과 쓰기". `run`을 이 둘 위에 다시 얹는다.
- `run_many`: 적중은 바로 채우고, 미스만 표시해 `run_batch`로 돌린다. 한 묶음 안의 같은 키는 한 번만 계산한다.
- `async_run`(설계 4.7).

확인:

- 여러 프로세스가 같은 키들을 동시에 `run`하면 키마다 단계가 한 번만 실행되고, 모두 같은 `result_sha`를 받는다.
- 표시를 쥔 프로세스를 SIGKILL로 죽이면 기다리던 프로세스가 바로 가져가 실행한다.
- 표시를 쥔 쪽이 예외로 끝나면 기다리던 쪽이 다시 시도한다.
- `fork`한 자식은 부모의 잠금 파일을 쓰지 않는다.
- `run_many`는 입력 순서대로 결과를 돌려준다. 일부가 예외여도 나머지는 저장되고, 예외는 `res.error`에 담긴다.
- 같은 키의 `async_run` 10개를 `gather`하면 단계는 한 번만 실행된다. 기다리는 동안 이벤트 루프는 다른 일을 한다.
- `async def` 단계를 `run`으로 부르면 오류다.
- 여러 스레드가 한 `StepCache`를 같이 써도 위 결과가 같다.

### M3. 매니페스트와 stale

만들 것:

- `cache.subject(name, source=)`: `run`, `run_many`, `async_run`. 블록이 정상으로 끝날 때만 매니페스트를 바꾼다.
- `Step(cache=False)`: 실행만 하고 `(단계, 단계 버전)`을 매니페스트에 남긴다.
- `cache.stale_subjects(pipeline, complete)`: 이유는 `version`(달라진 구성 요소 포함), `record`, `failed`, `missing_step`.
- `cache.manifest(subject)`: subject의 최신 매니페스트. 없으면 `None`.

확인:

- 설계 5.2의 예를 그대로 테스트한다. 키 K를 무효화하면 A와 B가 stale이 된다. B를 다시 처리하면 B만 stale에서 빠진다. A를 다시 처리하면 K는 적중하고 단계는 실행되지 않는다.
- 단계 버전을 바꾸면 그 단계를 쓴 subject가 stale이 되고, 이유에 달라진 구성 요소가 나온다. `cache=False` 단계도 같다.
- 블록이 예외로 끝나면 이전 매니페스트가 그대로 남는다.
- 블록 안에서 잡은 실패(`run_many`의 오류, `s.run`·`s.async_run`의 예외)는 `failed`로 나온다. `cache=False` 단계도 같고, 키는 null이다. 같은 블록에서 다시 시도해 성공하면 stale이 아니다.
- `complete=True`일 때만 매니페스트에 없는 단계가 `missing_step`으로 나온다. `pipeline`에 없는 단계의 항목은 무시된다.
- 여러 스레드가 한 subject 세션을 같이 써도 항목이 빠지지 않는다.

### M4. 0.1.0 배포

- mac(로컬)과 ARM Linux(CI)에서 전체 테스트를 통과시킨다.
- README를 쓴다: 설치, 짧은 예제, 지원 환경, "호스트 프로세스와 컨테이너가 같은 캐시를 함께 쓰지 않는다"(설계 6.1).
- `uv build` 뒤 패키지 색인에 올린다. 올리기 전에 확인을 받는다.

## 4. CI

- M1의 PR에 GitHub Actions 워크플로를 넣는다. PR마다 ARM Linux 러너에서 `uv run pytest`를 돌린다. 러너 이름은 넣을 때 확인한다.
- mac에서는 개발하면서 로컬로 돌린다. 워크플로에 macOS는 넣지 않는다.

## 5. 배포 뒤에 확인할 것

설계 11장의 "구현 때 확인할 것"이다. 둘 다 사용처의 데이터나 환경이 필요해서 0.1.0을 막지 않는다.

- blob 기준값(4 KiB, 256 KiB): 첫 사용처의 실제 출력 크기 분포로 확인한다. 값을 바꾸더라도 저장된 것은 그대로 읽힌다.
- 운영 환경의 잠금: 운영 머신의 파일 시스템에서 M2의 경합 테스트를 돌린다.

## 6. 테스트 돌리기

```
uv run pytest                                             # mac, Linux
docker run --rm --platform linux/arm64 -v "$PWD":/src:ro python:3.12-slim \
  sh -c 'cp -r /src /work && cd /work && rm -rf .venv && pip install -q pytest -e . && pytest'   # ARM Linux
```

컨테이너 안에서는 소스를 복사해 컨테이너의 파일 시스템에서 돌린다. 테스트 DB가 mac 폴더의 bind mount 위에 놓이지 않게 하려는 것이다.
