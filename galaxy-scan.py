#!/usr/bin/env python3

# Copyright 2026 Pouria Rezaei <Pouria.rz@outlook.com>
# All rights reserved.
#
# Redistribution and use of this script, with or without modification, is
# permitted provided that the following conditions are met:
#
# 1. Redistributions of this script must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
#  THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
#  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
#  EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
#  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
#  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
#  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
#  OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import ipaddress
import maxminddb

DB_PATH = "GeoLite2-City.mmdb"
PARALLEL_SCAN = 14

COUNTRY_CODES = {
	"AD", "AE", "AF", "AG", "AL", "AO", "AQ", "AR", "AS", "ZM",
	"AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF",
	"BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BR", "BS",
	"BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG",
	"CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV",
	"CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ",
	"EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK",
	"FM", "FO", "FR", "GA", "GB", "GD", "GF", "GG", "GH", "GI",
	"GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW",
	"GY", "HK", "HM", "HN", "HR", "HT", "HU", "ID", "IE", "IL",
	"IM", "IN", "IO", "IQ", "IS", "IT", "JE", "JM", "JO", "ZW",
	"JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW",
	"KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LU",
	"LV", "LY", "MA", "MC", "MD", "MF", "MG", "MH", "MK", "ML",
	"MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV",
	"MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF", "NG", "NI",
	"NL", "NO", "NP", "NR", "NU", "NZ", "PA", "PE", "PF", "PG",
	"PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY",
	"QA", "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD",
	"SE", "SG", "SH", "SI", "SJ", "SL", "SM", "SN", "SO", "SR",
	"SS", "ST", "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG",
	"TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR", "TT", "TV",
	"TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC",
	"VE", "VG", "VI", "VN", "VU", "WF", "WS", "YE", "YT", "ZA",
}

def country_iso(record):
	if not record:
		return None
	for key in ("country", "registered_country", "represented_country"):
		v = record.get(key)
		if isinstance(v, dict):
			iso = v.get("iso_code") or v.get("iso")
			if iso:
				return iso.upper()
	return None


def aggregate_to_24(network: ipaddress.IPv4Network) -> ipaddress.IPv4Network:
	if network.version != 4 or network.prefixlen < 25:
		return network

	supernet = network.supernet(new_prefix=24)
	return ipaddress.ip_network(f"{supernet.network_address}/24")


def sort_key(net):
	return (net.version, int(net.network_address), net.prefixlen)


def collapse_and_sort(networks):
	v4 = [net for net in networks if isinstance(net, ipaddress.IPv4Network)]
	v6 = [net for net in networks if isinstance(net, ipaddress.IPv6Network)]

	collapsed_v4 = sorted(ipaddress.collapse_addresses(v4), key=sort_key)
	collapsed_v6 = sorted(ipaddress.collapse_addresses(v6), key=sort_key)

	return collapsed_v4, collapsed_v6


def write_output(output_file: Path, v4_networks, v6_networks) -> None:
	with output_file.open("w", encoding="utf-8", newline="\n") as f:
		for net in v4_networks:
			f.write(f"{net.with_prefixlen}\n")
		for net in v6_networks:
			f.write(f"{net.with_prefixlen}\n")


def process_country(code: str, networks: list[ipaddress.IPv4Network]) -> dict:
	fname = Path(f"{code.lower()}.txt")

	v4_before = len(networks)
	v6_before = 0

	if not networks:
		fname.write_text("", encoding="utf-8")
		return {
			"code": code,
			"file": fname.name,
			"v4_before": v4_before,
			"v6_before": v6_before,
			"final_count": 0,
			"invalid_count": 0,
			"skipped_count": 0,
		}

	v4_collapsed, v6_collapsed = collapse_and_sort(networks)
	write_output(fname, v4_collapsed, v6_collapsed)

	final_count = len(v4_collapsed) + len(v6_collapsed)

	return {
		"code": code,
		"file": fname.name,
		"v4_before": v4_before,
		"v6_before": v6_before,
		"final_count": final_count,
		"invalid_count": 0,
		"skipped_count": 0,
	}


def scan_database(db_path: str):
	country_buckets: dict[str, list[ipaddress.IPv4Network]] = defaultdict(list)
	total_raw_ipv4 = 0
	seen_countries = set()

	print(f"[SCAN] Opening database: {db_path}", flush=True)

	with maxminddb.open_database(db_path) as reader:
		for index, (network, record) in enumerate(reader, start=1):
			if network.version != 4:
				continue

			iso = country_iso(record)
			if iso not in COUNTRY_CODES:
				continue

			cleaned = aggregate_to_24(network)
			country_buckets[iso].append(cleaned)
			total_raw_ipv4 += 1

			if iso not in seen_countries:
				seen_countries.add(iso)
				print(
					f"[SCAN] First country bucket seen: {iso} "
					f"({len(country_buckets[iso]):,} network(s) so far)",
					flush=True,
				)

			if total_raw_ipv4 % 50_000 == 0:
				print(
					f"[SCAN] Processed {total_raw_ipv4:,} raw IPv4 networks "
					f"across {len(country_buckets):,} countries...",
					flush=True,
				)

	print(
		f"[SCAN] Done. Collected {total_raw_ipv4:,} raw IPv4 networks "
		f"across {len(country_buckets):,} countries.",
		flush=True,
	)
	return country_buckets, total_raw_ipv4


def main(db_path=DB_PATH):
	country_buckets, total_raw = scan_database(db_path)

	print(
		f"\n[POST] Collapsing and writing country files in parallel "
		f"({PARALLEL_SCAN} countries at a time)...",
		flush=True,
	)

	total_final_networks = 0
	futures = {}

	with ThreadPoolExecutor(max_workers=PARALLEL_SCAN) as executor:
		for code in sorted(country_buckets.keys()):
			networks = country_buckets[code]
			print(
				f"[POST] Queueing {code} "
				f"({len(networks):,} raw network(s))",
				flush=True,
			)
			futures[executor.submit(process_country, code, networks)] = code

		completed = 0
		for future in as_completed(futures):
			stats = future.result()
			completed += 1
			total_final_networks += stats["final_count"]

			print(
				f"[POST] {completed:,}/{len(futures):,} "
				f"{stats['code']} => {stats['file']}  "
				f"({stats['v4_before']:,} IPv4 + {stats['v6_before']:,} IPv6 before collapse, "
				f"{stats['final_count']:,} final networks)",
				flush=True,
			)

	print(
		f"\n[DONE] Finished! Processed {total_raw:,} raw IPv4 networks → "
		f"{total_final_networks:,} final collapsed/sorted network(s) "
		f"across {len(country_buckets):,} countries.",
		flush=True,
	)


if __name__ == "__main__":
	main()
