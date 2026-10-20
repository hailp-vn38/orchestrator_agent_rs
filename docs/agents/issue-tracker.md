# Issue tracker: GitHub

Issues và specs được quản lý bằng GitHub Issues.
Dùng CLI `gh` trong repo; xác định repository từ git remote.

## Thao tác

- Đọc: `gh issue view <number> --comments`.
- Liệt kê: `gh issue list --state open --json number,title,body,labels`.
- Tạo: `gh issue create --title "..." --body-file <file>`.
- Bình luận: `gh issue comment <number> --body-file <file>`.
- Gắn nhãn: `gh issue edit <number> --add-label "<label>"`.
- Bỏ nhãn: `gh issue edit <number> --remove-label "<label>"`.
- Đóng: `gh issue close <number>`.

Với nội dung nhiều dòng, ghi nội dung vào file rồi dùng `--body-file`.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Quy ước của skills

- "Publish to the issue tracker": tạo GitHub issue.
- "Fetch the relevant ticket": đọc issue cùng comments.

## Wayfinding

- Map là issue có nhãn `wayfinder:map`, chứa Notes, Decisions-so-far và Fog.
- Ticket con được liên kết bằng GitHub sub-issues. Nếu không hỗ trợ,
  dùng task list trong map và dòng `Part of #<map>` trong ticket.
- Ticket con dùng nhãn `wayfinder:<type>`:
  `research`, `prototype`, `grilling` hoặc `task`.
- Quan hệ chặn dùng native issue dependencies. Nếu không hỗ trợ,
  ghi `Blocked by: #<number>` trong ticket.
- Ticket sẵn sàng là ticket mở, chưa có assignee và mọi blocker đã đóng;
  chọn theo thứ tự trong map.
- Nhận việc bằng `gh issue edit <number> --add-assignee @me`.
- Khi giải quyết xong, bình luận kết quả, đóng ticket và thêm tóm tắt
  cùng liên kết vào Decisions-so-far của map.
