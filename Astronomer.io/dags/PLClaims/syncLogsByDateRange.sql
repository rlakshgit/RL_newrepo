query{syncLogsByDateRange(startDate: "start_date",
endDate: "end_date",
size:page_size,
page:page_count)
{size
page
count
syncLogs{sync_date
deleted
added
sync_type
}}}