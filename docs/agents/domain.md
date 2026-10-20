# Domain docs

## Đọc trước khi khám phá

Repo dùng bố cục single-context: `CONTEXT.md` và `docs/adr/`
tại root khi các tài liệu này được tạo.

Tài liệu hiện có:
- `docs/README.md`: mục lục thiết kế.
- `docs/14-glossary.md`: thuật ngữ domain.
- `docs/13-adrs.md`: các quyết định kiến trúc đã chấp nhận.

Đọc glossary và các ADR liên quan tới phần đang làm.
Nếu `CONTEXT.md` hoặc `docs/adr/` đã tồn tại, đọc thêm nội dung liên quan.

Nếu tài liệu theo bố cục chuẩn chưa tồn tại, tiếp tục với tài liệu hiện có.
Skill domain-modeling tạo tài liệu mới khi thuật ngữ hoặc quyết định
thực sự được chốt.

## Thuật ngữ

Dùng thuật ngữ trong glossary và CONTEXT.md khi đặt tên issue,
đề xuất refactor, viết giả thuyết hoặc đặt tên test.
Nếu thiếu khái niệm cần dùng, ghi nhận khoảng trống cho domain-modeling.

## Xung đột ADR

Nếu đề xuất mâu thuẫn ADR đã có, nêu rõ ADR bị ảnh hưởng và lý do
cần xem xét lại. Ghi quyết định thay thế theo quy ước supersede
trong `docs/13-adrs.md`.
