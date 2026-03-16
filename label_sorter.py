#!/usr/bin/env python3
"""
Shiprocket Label Sorter 2.0
============================
Sorts bulk shipping labels by Courier and SKU, outputs organized PDFs.
Now with Duplicate Order Filter & Duplicate Contact Detection.

Output format: YYYY-MM-DD_Courier_SKU.pdf

Author: Kluzo 😎 for Dhruv Shetty / JSK Labs
"""

import re
import os
import sys
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Add venv packages
sys.path.insert(0, '/Users/klaus/.openclaw/workspace/.venv/lib/python3.13/site-packages')

from pypdf import PdfReader, PdfWriter


def normalize_courier(courier_raw: str) -> str:
    """Normalize courier names for consistent file naming."""
    courier_lower = courier_raw.lower()
    
    if 'ekart' in courier_lower:
        return 'Ekart'
    elif 'delhivery' in courier_lower:
        return 'Delhivery'
    elif 'xpressbees' in courier_lower:
        return 'Xpressbees'
    elif 'bluedart' in courier_lower:
        return 'BlueDart'
    elif 'dtdc' in courier_lower:
        return 'DTDC'
    elif 'shadowfax' in courier_lower:
        return 'Shadowfax'
    elif 'ecom' in courier_lower:
        return 'EcomExpress'
    else:
        # Clean up for filename
        return re.sub(r'[^\w\-]', '', courier_raw.replace(' ', '-'))[:30]


def normalize_sku(sku_raw: str) -> str:
    """Normalize SKU for filename safety."""
    # Remove special chars, keep alphanumeric and hyphens
    return re.sub(r'[^\w\-]', '', sku_raw.replace(' ', '-'))[:50]


def extract_label_info(page_text: str) -> dict:
    """Extract courier, SKU, date, order ID, and phone from label text."""
    info = {
        'courier': 'Unknown',
        'sku': 'Unknown',
        'date': datetime.now().strftime('%Y-%m-%d'),
        'order_id': None,
        'phone': None,
        'customer_name': None,
    }
    
    # Courier patterns (order matters - more specific first)
    courier_patterns = [
        (r'Ekart[^\n]*', 'Ekart'),
        (r'Delhivery[^\n]*', 'Delhivery'),
        (r'Xpressbees[^\n]*', 'Xpressbees'),
        (r'BlueDart[^\n]*', 'BlueDart'),
        (r'DTDC[^\n]*', 'DTDC'),
        (r'Shadowfax[^\n]*', 'Shadowfax'),
        (r'Ecom\s*Express[^\n]*', 'EcomExpress'),
    ]
    
    for pattern, name in courier_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            info['courier'] = name
            break
    
    # SKU extraction
    sku_match = re.search(r'SKU:\s*([^\n]+)', page_text)
    if sku_match:
        info['sku'] = normalize_sku(sku_match.group(1).strip())
    
    # Date extraction (Invoice Date preferred)
    date_match = re.search(r'Invoice Date:\s*(\d{4}-\d{2}-\d{2})', page_text)
    if date_match:
        info['date'] = date_match.group(1)
    else:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', page_text)
        if date_match:
            info['date'] = date_match.group(1)
    
    # Order ID extraction
    order_patterns = [
        r'Order\s*(?:No|ID|#)[.:;]?\s*([A-Za-z0-9\-_]+)',
        r'Order[:\s]+([A-Za-z0-9\-_]+)',
        r'order_id[:\s]+([A-Za-z0-9\-_]+)',
    ]
    for pat in order_patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            info['order_id'] = m.group(1).strip()
            break
    
    # Phone number extraction (Indian 10-digit numbers)
    phone_patterns = [
        r'(?:Phone|Mobile|Contact|Tel|Ph)[.:;]?\s*\+?91?\s*[-.]?\s*(\d{10})',
        r'(?:Phone|Mobile|Contact|Tel|Ph)[.:;]?\s*(\d{10})',
        r'\b(\d{10})\b',
    ]
    for pat in phone_patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            phone = m.group(1).strip()
            if len(phone) == 10 and phone[0] in '6789':
                info['phone'] = phone
                break
    
    # Customer name extraction
    name_match = re.search(r'(?:Customer|Deliver(?:y)?\s*To|Ship\s*To|Name)[.:;]?\s*([A-Za-z ]+)', page_text, re.IGNORECASE)
    if name_match:
        info['customer_name'] = name_match.group(1).strip()
    
    return info


def sort_labels(input_pdf: str, output_dir: str = None, filter_duplicates: bool = True) -> dict:
    """
    Sort labels from input PDF into separate PDFs by Courier + SKU.
    Also detects duplicate orders and duplicate contact numbers.
    
    Args:
        input_pdf: Path to the input PDF with bulk labels
        output_dir: Directory for output PDFs (default: same as input)
        filter_duplicates: If True, removes duplicate order labels from sorted output
    
    Returns:
        dict with summary of created files and duplicate info
    """
    input_path = Path(input_pdf)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_pdf}")
    
    if output_dir is None:
        output_dir = input_path.parent / 'sorted_labels'
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📄 Reading: {input_path.name}")
    reader = PdfReader(str(input_path))
    total_pages = len(reader.pages)
    print(f"   Found {total_pages} labels")
    
    # Phase 1: Extract info from all pages
    all_labels = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        info = extract_label_info(text)
        info['page_index'] = i
        all_labels.append(info)
        
        if (i + 1) % 50 == 0:
            print(f"   Processed {i + 1}/{total_pages} labels...")
    
    # Phase 2: Detect duplicate orders
    order_id_map = defaultdict(list)
    for idx, label in enumerate(all_labels):
        if label['order_id']:
            order_id_map[label['order_id']].append(idx)
    
    duplicate_order_ids = {oid: indices for oid, indices in order_id_map.items() if len(indices) > 1}
    
    if duplicate_order_ids:
        print(f"\n⚠️  Found {len(duplicate_order_ids)} duplicate order(s):")
        for oid, indices in duplicate_order_ids.items():
            pages = ', '.join(str(all_labels[i]['page_index'] + 1) for i in indices)
            print(f"   Order {oid}: appears {len(indices)} times (pages {pages})")
    
    # Phase 3: Detect duplicate contact numbers
    phone_map = defaultdict(list)
    for label in all_labels:
        if label['phone']:
            phone_map[label['phone']].append(label)
    
    duplicate_phones = {phone: labels for phone, labels in phone_map.items() if len(labels) > 1}
    
    if duplicate_phones:
        print(f"\n📱 Found {len(duplicate_phones)} duplicate contact number(s):")
        for phone, labels in duplicate_phones.items():
            order_ids = ', '.join(filter(None, [l.get('order_id', 'N/A') for l in labels]))
            print(f"   {phone}: {len(labels)} orders ({order_ids})")
    
    # Phase 4: Build filtered set (remove duplicate orders if enabled)
    duplicate_page_indices = set()
    if filter_duplicates and duplicate_order_ids:
        for oid, indices in duplicate_order_ids.items():
            for dup_idx in indices[1:]:
                duplicate_page_indices.add(all_labels[dup_idx]['page_index'])
        print(f"\n🗑️  Removing {len(duplicate_page_indices)} duplicate label(s) from output")
    
    # Phase 5: Group pages by (date, courier, sku)
    groups = defaultdict(list)
    for label in all_labels:
        if label['page_index'] not in duplicate_page_indices:
            key = (label['date'], label['courier'], label['sku'])
            groups[key].append(label['page_index'])
    
    print(f"\n📦 Found {len(groups)} unique groups")
    
    # Phase 6: Create output PDFs
    results = []
    
    for (date, courier, sku), page_indices in sorted(groups.items()):
        filename = f"{date}_{courier}_{sku}.pdf"
        output_path = output_dir / filename
        
        writer = PdfWriter()
        for idx in page_indices:
            writer.add_page(reader.pages[idx])
        
        with open(output_path, 'wb') as f:
            writer.write(f)
        
        result = {
            'file': filename,
            'date': date,
            'courier': courier,
            'sku': sku,
            'labels': len(page_indices)
        }
        results.append(result)
        print(f"   ✅ {filename} ({len(page_indices)} labels)")
    
    # Phase 7: Save duplicate orders PDF
    if duplicate_page_indices:
        dup_writer = PdfWriter()
        for page_idx in sorted(duplicate_page_indices):
            dup_writer.add_page(reader.pages[page_idx])
        
        dup_path = output_dir / "_DUPLICATE_ORDERS.pdf"
        with open(dup_path, 'wb') as f:
            dup_writer.write(f)
        print(f"   🔁 _DUPLICATE_ORDERS.pdf ({len(duplicate_page_indices)} labels)")
    
    # Phase 8: Save duplicate contacts CSV
    if duplicate_phones:
        csv_path = output_dir / "_DUPLICATE_CONTACTS.csv"
        with open(csv_path, 'w', newline='') as f:
            writer_csv = csv.writer(f)
            writer_csv.writerow(['Phone Number', 'Occurrences', 'Order IDs', 'Customer Names', 'SKUs', 'Couriers'])
            
            for phone, labels in sorted(duplicate_phones.items(), key=lambda x: len(x[1]), reverse=True):
                order_ids = ', '.join(filter(None, [l.get('order_id', '') for l in labels]))
                names = ', '.join(filter(None, [l.get('customer_name', '') for l in labels]))
                skus = ', '.join(set(filter(None, [l.get('sku', '') for l in labels])))
                couriers = ', '.join(set(filter(None, [l.get('courier', '') for l in labels])))
                writer_csv.writerow([phone, len(labels), order_ids, names, skus, couriers])
        
        print(f"   📱 _DUPLICATE_CONTACTS.csv ({len(duplicate_phones)} numbers)")
    
    print(f"\n🎉 Done! {len(results)} files created in: {output_dir}")
    
    return {
        'input': str(input_path),
        'output_dir': str(output_dir),
        'total_labels': total_pages,
        'files': results,
        'duplicates': {
            'duplicate_order_count': len(duplicate_order_ids),
            'duplicate_labels_removed': len(duplicate_page_indices),
            'duplicate_phone_count': len(duplicate_phones),
            'duplicate_orders': {oid: len(indices) for oid, indices in duplicate_order_ids.items()},
            'duplicate_phones': {phone: len(labels) for phone, labels in duplicate_phones.items()},
        }
    }


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Sort Shiprocket shipping labels by Courier and SKU'
    )
    parser.add_argument('input_pdf', help='Path to input PDF with bulk labels')
    parser.add_argument('-o', '--output', help='Output directory (default: ./sorted_labels)')
    parser.add_argument('--no-filter-duplicates', action='store_true',
                        help='Keep duplicate order labels in output (still reports them)')
    
    args = parser.parse_args()
    
    try:
        result = sort_labels(args.input_pdf, args.output, 
                            filter_duplicates=not args.no_filter_duplicates)
        
        print("\n" + "="*50)
        print("SUMMARY")
        print("="*50)
        print(f"Total labels processed: {result['total_labels']}")
        print(f"Output files created: {len(result['files'])}")
        print(f"Output directory: {result['output_dir']}")
        
        dup = result['duplicates']
        if dup['duplicate_order_count'] > 0:
            print(f"Duplicate orders found: {dup['duplicate_order_count']}")
            print(f"Duplicate labels removed: {dup['duplicate_labels_removed']}")
        if dup['duplicate_phone_count'] > 0:
            print(f"Duplicate contact numbers: {dup['duplicate_phone_count']}")
            print(f"  → See _DUPLICATE_CONTACTS.csv")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
