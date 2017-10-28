SELECT
    u.uid, SUBSTRING_INDEX(u.email, '@', -1) as email,
    ui.gender
FROM users as u
    LEFT JOIN users_info as ui ON ui.uid = u.uid
WHERE ui.country = 'CA'
--  ORDER BY uid DESC
--  LIMIT 10000
