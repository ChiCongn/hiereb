Hãy review repo như senior research software engineer và reviewer khó tính.

Đọc `AGENTS.md`, toàn bộ `docs/`, `IMPLEMENTATION_PLAN.md`, `IMPLEMENTATION_REPORT.md`, source và tests.

Chạy:
```bash
uv sync
uv run ruff check .
uv run mypy src
uv run pytest
uv run hiereb generate-synthetic --output data/synthetic_house_0.csv
uv run hiereb compare --config configs/house_0.example.yaml
uv run hiereb verify-legacy-equivalence --config configs/house_0.example.yaml
```

Kiểm tra:
1. Temporal semantics và leakage.
2. Legacy two-stage có đúng bằng flat.
3. True hierarchy có thật sự phụ thuộc household grouping.
4. Cap redistribution: không vượt cap/budget, terminate, báo unused.
5. Suppression invariant và house bound.
6. Metrics có aggregation đúng.
7. Determinism: chạy 2 lần, so checksum artifact sau khi bỏ field runtime.
8. Config validation.
9. Failure tests, không chỉ happy path.
10. TODO, placeholder, silent fallback, claim không được code hỗ trợ.

Nếu thấy lỗi, sửa, thêm regression test, chạy lại toàn bộ gate.

Tạo `REVIEW_REPORT.md` gồm findings Critical/High/Medium/Low, fix, tests mới, gate result, rủi ro còn lại.
