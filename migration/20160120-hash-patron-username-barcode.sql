CREATE EXTENSION pgcrypto;
ALTER TABLE patrons ADD COLUMN authorization_identifier_hashed varchar;
ALTER TABLE patrons ADD COLUMN username_hashed varchar;
UPDATE patrons SET authorization_identifier_hashed = crypt(authorization_identifier, gen_salt('bf', 8));
UPDATE patrons SET username_hashed = crypt(username, gen_salt('bf', 8));
ALTER TABLE patrons DROP COLUMN authorization_identifier;
ALTER TABLE patrons DROP COLUMN username; 