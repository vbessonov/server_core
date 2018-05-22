-- Update all representations that include book-covers.nypl.org to
-- point to covers.nypl.org instead. While we're at it, clean up any
-- ugly 's3.amazonaws.com' references.
update representations set mirror_url=replace(replace(replace(replace(mirror_url, 'https://s3.amazonaws.com/book-covers.nypl.org/', 'https://covers.nypl.org/'), 'http://s3.amazonaws.com/book-covers.nypl.org/', 'https://covers.nypl.org/'), 'https://book-covers.nypl.org/', 'https://covers.nypl.org/'), 'http://book-covers.nypl.org/', 'https://covers.nypl.org/'), url=replace(replace(replace(replace(url, 'https://s3.amazonaws.com/book-covers.nypl.org/', 'https://covers.nypl.org/'), 'http://s3.amazonaws.com/book-covers.nypl.org/', 'https://covers.nypl.org/'), 'https://book-covers.nypl.org/', 'https://covers.nypl.org/'), 'http://book-covers.nypl.org/', 'https://covers.nypl.org/') where mirror_url like '%book-covers.nypl.org%' or url like '%book-covers.nypl.org%';

-- All OPDS entries that include an old link need to be
-- updated. Rather than trying to figure out which ones need to be
-- updated (which would be very expensive) just mark them all for an
-- update. They'll be regenerated in the background.
delete from workcoveragerecords where operation='generate-opds';
