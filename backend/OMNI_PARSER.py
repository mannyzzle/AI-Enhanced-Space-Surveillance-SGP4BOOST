from datetime import datetime, timedelta

# Base URL format for AGI's TLE archive
tle_url_template = "https://support.agi.com/download/?type=agilelimelight&file=stkAllTLE{date}.tar.gz&dir=SatDbArchive/{year}"

# Start and end dates
start_date = datetime(2019, 2, 19)
end_date = datetime(2025, 12, 25)

# Output file to save links
output_file = "tle_download_links.txt"

# Generate all download links
links = []
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y%m%d")  # Format: YYYYMMDD
    year_str = current_date.strftime("%Y")      # Extract year
    tle_url = tle_url_template.format(date=date_str, year=year_str)
    links.append(tle_url)
    current_date += timedelta(days=1)

# Save the list to a file
with open(output_file, "w") as f:
    for link in links:
        f.write(link + "\n")

print(f"✅ Generated {len(links)} download links and saved to '{output_file}'.")
