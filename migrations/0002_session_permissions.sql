-- M2: thu hẹp quyền theo session (AT-39). Cột nullable trên bảng `sessions` hiện có;
-- không thêm bảng. NULL = kế thừa hoàn toàn quyền của AgentDefinition.
-- JSON: {"allowed_tools": [..]|null, "allowed_skills": [..]|null}. Policy chỉ giao
-- (thu hẹp) với quyền của agent, không bao giờ mở rộng.
ALTER TABLE sessions ADD COLUMN permissions_json TEXT
    CHECK (permissions_json IS NULL OR json_valid(permissions_json));
