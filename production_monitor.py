#!/usr/bin/env python3
"""
Production Multi-Agent Research Monitoring System
No LLM - Pure Automation with Advanced Filtering
Ready for KVM-1 VPS Deployment
"""

import requests
import time
import sqlite3
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from typing import List, Dict, Optional, TypedDict, Annotated
from datetime import datetime, timedelta
from pathlib import Path
import operator
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from langgraph.graph import StateGraph, END
from apscheduler.schedulers.blocking import BlockingScheduler

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================================
# CONFIGURATION
# ================================
class Config:
    # Search Configuration
    BASE_QUERY = "computer vision"
    LIMIT = 20
    
    # API Settings
    SEMANTIC_SCHOLAR_URL = "https://api.semanticscholar.org/graph/v1"
    ARXIV_API_URL = "http://export.arxiv.org/api/query"
    TIMEOUT = 10
    RATE_LIMIT = 1
    # Extra Semantic Scholar query to surface IEEE, ACM, Springer, arXiv venue coverage (same API as primary fetch)
    SUPPLEMENTARY_SS_SUFFIX = (
        ' (venue:IEEE OR venue:ACM OR venue:Springer OR venue:arXiv OR venue:"IEEE Access")'
    )
    SUPPLEMENTARY_SS_LIMIT_CAP = 30
    
    # Venue Rankings
    VENUE_RANKS = {
        "CVPR": "A*", "ICCV": "A*", "ECCV": "A*",
        "NeurIPS": "A*", "ICML": "A*", "ICLR": "A*",
        "AAAI": "A", "IJCAI": "A",
        "TPAMI": "Q1", "IJCV": "Q1", "TIP": "Q1",
        "BMVC": "B", "WACV": "B",
        "IEEE": "Q1", "ACM": "A", "Springer": "Q1", "arXiv": "Unranked",
    }
    
    
    DB_PATH = "research_papers.db"
    FILTERS_PATH = "user_filters.json"
    REPORT_DIR = Path("reports")
    
    # Email Configuration
    EMAIL_FROM = "your_email@gmail.com"
    EMAIL_PASSWORD = "your_app_password"  # Gmail app password
    EMAIL_TO = ["recipient@email.com"]
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    
    # Schedule
    RUN_HOUR = 9
    RUN_MINUTE = 0

# ================================
# USER FILTERS
# ================================
class UserFilters:
    """Manage user-defined filters for paper selection"""
    
    @staticmethod
    def load():
        """Load filters from JSON file"""
        if Path(Config.FILTERS_PATH).exists():
            with open(Config.FILTERS_PATH, 'r') as f:
                return json.load(f)
        
        # Default filters (all disabled)
        return {
            "enabled": True,
            "query": Config.BASE_QUERY,
            
            # Author filters
            "author_names": [],  # e.g., ["Yann LeCun", "Fei-Fei Li"]
            "author_institutions": [],  # e.g., ["MIT", "Stanford", "Google"]
            
            # Venue filters
            "venues": [],  # e.g., ["CVPR", "ICCV", "NeurIPS"]
            "min_venue_rank": None,  # "A*", "Q1", "A", "B", or None
            
            # Citation filters
            "min_citations": 0,
            "max_citations": None,
            
            # Date filters
            "min_year": None,
            "max_year": None,
            "last_n_days": None,  # Papers from last N days
            
            # Content filters
            "keywords_include": [],  # Must contain at least one
            "keywords_exclude": [],  # Must not contain any
            
            # Quantity
            "max_results": 20
        }
    
    @staticmethod
    def save(filters: Dict):
        """Save filters to file"""
        with open(Config.FILTERS_PATH, 'w') as f:
            json.dump(filters, f, indent=2)
        logger.info(f"Filters saved to {Config.FILTERS_PATH}")
    
    @staticmethod
    def apply_to_papers(papers: List[Dict], filters: Dict) -> List[Dict]:
        """Apply all filters to paper list"""
        if not filters.get("enabled", True):
            return papers
        
        filtered = papers
        
        # Author name filter
        if filters.get("author_names"):
            author_names_lower = [a.lower() for a in filters["author_names"]]
            filtered = [
                p for p in filtered
                if any(
                    any(filter_name in author.lower() for filter_name in author_names_lower)
                    for author in p.get("authors", [])
                )
            ]
        
        # Institution filter
        if filters.get("author_institutions"):
            institutions_lower = [i.lower() for i in filters["author_institutions"]]
            filtered = [
                p for p in filtered
                if any(
                    any(inst in detail.get("affiliation", "").lower() for inst in institutions_lower)
                    for detail in p.get("author_details", [])
                )
            ]
        
        # Venue filter
        if filters.get("venues"):
            venues_lower = [v.lower() for v in filters["venues"]]
            filtered = [
                p for p in filtered
                if any(venue in p.get("venue", "").lower() for venue in venues_lower)
            ]
        
        # Venue rank filter
        if filters.get("min_venue_rank"):
            rank_order = {"A*": 4, "Q1": 3, "A": 2, "B": 1, "Unranked": 0}
            min_rank_value = rank_order.get(filters["min_venue_rank"], 0)
            filtered = [
                p for p in filtered
                if rank_order.get(p.get("venue_rank", "Unranked"), 0) >= min_rank_value
            ]
        
        # Citation filters
        if filters.get("min_citations") is not None:
            filtered = [p for p in filtered if p.get("citations", 0) >= filters["min_citations"]]
        
        if filters.get("max_citations") is not None:
            filtered = [p for p in filtered if p.get("citations", 0) <= filters["max_citations"]]
        
        # Year filters
        if filters.get("min_year"):
            filtered = [p for p in filtered if p.get("year", 0) >= filters["min_year"]]
        
        if filters.get("max_year"):
            filtered = [p for p in filtered if p.get("year", 9999) <= filters["max_year"]]
        
        # Last N days filter
        if filters.get("last_n_days"):
            cutoff_date = datetime.now() - timedelta(days=filters["last_n_days"])
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            filtered = [
                p for p in filtered
                if p.get("publication_date", "9999-12-31") >= cutoff_str
            ]
        
        # Keyword inclusion
        if filters.get("keywords_include"):
            keywords_lower = [k.lower() for k in filters["keywords_include"]]
            filtered = [
                p for p in filtered
                if any(
                    keyword in p.get("title", "").lower() or 
                    keyword in p.get("abstract", "").lower()
                    for keyword in keywords_lower
                )
            ]
        
        # Keyword exclusion
        if filters.get("keywords_exclude"):
            keywords_lower = [k.lower() for k in filters["keywords_exclude"]]
            filtered = [
                p for p in filtered
                if not any(
                    keyword in p.get("title", "").lower() or 
                    keyword in p.get("abstract", "").lower()
                    for keyword in keywords_lower
                )
            ]
        
        # Max results
        if filters.get("max_results"):
            filtered = filtered[:filters["max_results"]]
        
        return filtered

# ================================
# STATE DEFINITION
# ================================
class ResearchState(TypedDict):
    filters: Dict
    papers: Annotated[List[Dict], operator.add]
    new_papers: List[Dict]
    filtered_papers: List[Dict]
    report_path: Optional[str]
    email_sent: bool
    error: Optional[str]
    stats: Dict
    timestamp: str

# ================================
# FETCH HELPERS (arXiv + Semantic Scholar; same paper shape as before)
# ================================

def _structure_ss_paper(paper: Dict) -> Dict:
    """Map Semantic Scholar paper JSON to pipeline dict."""
    authors = []
    author_ids = []
    for a in paper.get("authors", []):
        authors.append(a.get("name", "Unknown"))
        if a.get("authorId"):
            author_ids.append(a["authorId"])
    return {
        "paper_id": paper.get("paperId"),
        "title": paper.get("title", "Unknown"),
        "authors": authors,
        "author_ids": author_ids,
        "venue": paper.get("venue"),
        "year": paper.get("year"),
        "publication_date": paper.get("publicationDate"),
        "citations": paper.get("citationCount", 0),
        "abstract": (paper.get("abstract") or "")[:500],
        "author_details": [],
        "venue_rank": "Unknown",
        "is_new": True,
    }


def _dedupe_by_paper_id(papers: List[Dict]) -> List[Dict]:
    seen = set()
    out: List[Dict] = []
    for p in papers:
        pid = p.get("paper_id")
        if pid:
            if pid in seen:
                continue
            seen.add(pid)
        out.append(p)
    return out


def _semantic_scholar_search(search_query: str, filters: Dict, limit: int) -> List[Dict]:
    url = f"{Config.SEMANTIC_SCHOLAR_URL}/paper/search"
    params = {
        "query": search_query,
        "limit": limit,
        "fields": "paperId,title,authors,year,venue,citationCount,publicationDate,abstract",
        "year": f"{filters.get('min_year', 2000)}-{filters.get('max_year', 2030)}"
        if filters.get("min_year") or filters.get("max_year")
        else None,
    }
    params = {k: v for k, v in params.items() if v is not None}
    response = requests.get(url, params=params, timeout=Config.TIMEOUT)
    response.raise_for_status()
    raw_papers = response.json().get("data", [])
    return [_structure_ss_paper(p) for p in raw_papers]


def _parse_arxiv_atom(xml_text: str) -> List[Dict]:
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(xml_text)
    out: List[Dict] = []
    for entry in root.findall("atom:entry", ns):
        id_el = entry.find("atom:id", ns)
        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)
        summary_el = entry.find("atom:summary", ns)
        if id_el is None or not id_el.text:
            continue
        id_url = id_el.text.strip()
        m = re.search(r"arxiv\.org/abs/([^/?#]+)", id_url, re.I)
        arxiv_id = m.group(1) if m else id_url.rsplit("/", 1)[-1]
        title = (title_el.text or "").replace("\n", " ").strip() if title_el is not None else "Unknown"
        published = (published_el.text or "")[:10] if published_el is not None else None
        year = None
        if published and len(published) >= 4:
            try:
                year = int(published[:4])
            except ValueError:
                pass
        abstract = ""
        if summary_el is not None and summary_el.text:
            abstract = summary_el.text.replace("\n", " ").strip()[:500]
        authors = []
        for author in entry.findall("atom:author", ns):
            name_el = author.find("atom:name", ns)
            if name_el is not None and name_el.text:
                authors.append(name_el.text.strip())
        out.append(
            {
                "paper_id": f"arxiv:{arxiv_id}",
                "title": title,
                "authors": authors,
                "author_ids": [],
                "venue": "arXiv",
                "year": year,
                "publication_date": published,
                "citations": 0,
                "abstract": abstract,
                "author_details": [],
                "venue_rank": "Unknown",
                "is_new": True,
            }
        )
    return out


def _fetch_arxiv_papers(query: str, max_results: int) -> List[Dict]:
    if max_results <= 0:
        return []
    params = {
        "search_query": f"all:{query}",
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = f"{Config.ARXIV_API_URL}?{urlencode(params)}"
    headers = {"User-Agent": "research-paper-automation/1.0"}
    response = requests.get(url, headers=headers, timeout=Config.TIMEOUT)
    response.raise_for_status()
    return _parse_arxiv_atom(response.text)


# ================================
# AGENT NODES
# ================================

def fetch_papers_node(state: ResearchState) -> ResearchState:
    """
    Agent 1: Fetch papers from Semantic Scholar (primary + IEEE/ACM/Springer/arXiv venue supplement) and arXiv API.
    """
    logger.info(
        "🔍 Agent 1: Fetching papers (Semantic Scholar, arXiv API, supplementary IEEE/ACM/Springer/arXiv venues)..."
    )

    filters = state["filters"]
    query = filters.get("query", Config.BASE_QUERY)
    max_limit = min(filters.get("max_results", 20) * 2, 100)

    search_query = query
    if filters.get("venues") and len(filters["venues"]) <= 3:
        venue_filter = " OR ".join([f'venue:"{v}"' for v in filters["venues"]])
        search_query = f"{query} ({venue_filter})"

    try:
        structured = _semantic_scholar_search(search_query, filters, max_limit)
        logger.info(f"Semantic Scholar (primary): {len(structured)} papers")
    except Exception as e:
        logger.error(f"❌ Fetch failed: {e}")
        return {"papers": [], "error": str(e)}

    time.sleep(Config.RATE_LIMIT)
    try:
        arxiv_max = min(max_limit // 2, 25)
        arxiv_papers = _fetch_arxiv_papers(query, arxiv_max)
        structured.extend(arxiv_papers)
        logger.info(f"arXiv API: +{len(arxiv_papers)} papers")
    except Exception as e:
        logger.warning(f"arXiv fetch skipped: {e}")

    time.sleep(Config.RATE_LIMIT)
    try:
        sup_query = f"{query}{Config.SUPPLEMENTARY_SS_SUFFIX}"
        sup_limit = min(Config.SUPPLEMENTARY_SS_LIMIT_CAP, max_limit)
        sup_papers = _semantic_scholar_search(sup_query, filters, sup_limit)
        structured.extend(sup_papers)
        logger.info(f"Semantic Scholar (IEEE/ACM/Springer/arXiv venues): +{len(sup_papers)} papers")
    except Exception as e:
        logger.warning(f"Supplementary Semantic Scholar fetch skipped: {e}")

    structured = _dedupe_by_paper_id(structured)
    logger.info(f"✅ Fetched {len(structured)} unique papers (merged)")
    return {"papers": structured, "error": None}


def filter_duplicates_node(state: ResearchState) -> ResearchState:
    """
    Agent 2: Filter out papers already in database
    """
    logger.info("🔍 Agent 2: Filtering duplicate papers...")
    
    papers = state["papers"]
    
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    # Create tables if not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            paper_id TEXT PRIMARY KEY,
            title TEXT,
            venue TEXT,
            year INTEGER,
            citations INTEGER,
            venue_rank TEXT,
            publication_date TEXT,
            discovered_date TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            author_name TEXT,
            paper_id TEXT,
            h_index INTEGER,
            paper_count INTEGER,
            total_citations INTEGER,
            affiliation TEXT,
            FOREIGN KEY(paper_id) REFERENCES papers(paper_id)
        )
    """)
    
    cursor.execute("SELECT paper_id FROM papers")
    existing_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    # Filter new papers
    new_papers = [p for p in papers if p["paper_id"] not in existing_ids]
    
    logger.info(f"✅ Found {len(new_papers)} new papers (out of {len(papers)} total)")
    return {"new_papers": new_papers, "papers": papers}


def enrich_authors_node(state: ResearchState) -> ResearchState:
    """
    Agent 3: Enrich author details (only for papers that pass initial filters)
    """
    logger.info("👥 Agent 3: Enriching author profiles...")
    
    papers = state["new_papers"]
    filters = state["filters"]
    
    # Skip if no institution filter (save API calls)
    if not filters.get("author_institutions"):
        logger.info("⏭️  Skipping author enrichment (no institution filter)")
        return {"new_papers": papers}
    
    enriched_count = 0
    
    for paper in papers:
        # Only enrich first 3 authors to save API calls
        for author_id in paper.get("author_ids", [])[:3]:
            time.sleep(Config.RATE_LIMIT)
            
            url = f"{Config.SEMANTIC_SCHOLAR_URL}/author/{author_id}"
            params = {"fields": "name,hIndex,paperCount,citationCount,affiliations"}
            
            try:
                response = requests.get(url, params=params, timeout=Config.TIMEOUT)
                response.raise_for_status()
                
                data = response.json()
                affiliations = data.get("affiliations", [])
                
                paper["author_details"].append({
                    "name": data.get("name", "Unknown"),
                    "h_index": data.get("hIndex", 0),
                    "paper_count": data.get("paperCount", 0),
                    "citations": data.get("citationCount", 0),
                    "affiliation": affiliations[0] if affiliations else "Unknown"
                })
                enriched_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️  Failed to enrich author {author_id}: {e}")
    
    logger.info(f"✅ Enriched {enriched_count} author profiles")
    return {"new_papers": papers}


def apply_filters_node(state: ResearchState) -> ResearchState:
    """
    Agent 4: Apply user-defined filters
    """
    logger.info("🔍 Agent 4: Applying user filters...")
    
    papers = state["new_papers"]
    filters = state["filters"]
    
    filtered = UserFilters.apply_to_papers(papers, filters)
    
    logger.info(f"✅ {len(filtered)} papers passed filters (from {len(papers)})")
    
    # Calculate stats
    stats = {
        "total_fetched": len(state.get("papers", [])),
        "new_papers": len(papers),
        "after_filters": len(filtered),
        "filtered_out": len(papers) - len(filtered)
    }
    
    return {"filtered_papers": filtered, "stats": stats}


def analyze_impact_node(state: ResearchState) -> ResearchState:
    """
    Agent 5: Analyze venue impact factors
    """
    logger.info("📊 Agent 5: Analyzing venue impact...")
    
    papers = state["filtered_papers"]
    
    for paper in papers:
        venue = paper.get("venue", "")
        rank = "Unranked"
        
        if venue:
            venue_lower = venue.lower()
            for conf, conf_rank in Config.VENUE_RANKS.items():
                if conf.lower() in venue_lower:
                    rank = conf_rank
                    break
        
        paper["venue_rank"] = rank
    
    logger.info("✅ Impact analysis complete")
    return {"filtered_papers": papers}


def store_data_node(state: ResearchState) -> ResearchState:
    """
    Agent 6: Store papers in database
    """
    logger.info("💾 Agent 6: Storing papers in database...")
    
    papers = state["filtered_papers"]
    
    if not papers:
        logger.info("No papers to store")
        return {}
    
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    discovered_date = datetime.now().strftime("%Y-%m-%d")
    stored_count = 0
    
    for paper in papers:
        try:
            # Insert paper
            cursor.execute("""
                INSERT OR IGNORE INTO papers 
                (paper_id, title, venue, year, citations, venue_rank, publication_date, discovered_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper["paper_id"],
                paper["title"],
                paper.get("venue"),
                paper.get("year"),
                paper["citations"],
                paper["venue_rank"],
                paper.get("publication_date"),
                discovered_date
            ))
            
            # Insert authors
            for author in paper.get("author_details", []):
                cursor.execute("""
                    INSERT INTO authors 
                    (author_name, paper_id, h_index, paper_count, total_citations, affiliation)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    author["name"],
                    paper["paper_id"],
                    author["h_index"],
                    author["paper_count"],
                    author["citations"],
                    author["affiliation"]
                ))
            
            stored_count += 1
            
        except sqlite3.Error as e:
            logger.error(f"Failed to store {paper['paper_id']}: {e}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"✅ Stored {stored_count} papers")
    return {}


def generate_excel_node(state: ResearchState) -> ResearchState:
    """
    Agent 7: Generate Excel report
    """
    logger.info("📊 Agent 7: Generating Excel report...")
    
    papers = state["filtered_papers"]
    stats = state.get("stats", {})
    filters = state["filters"]
    
    if not papers:
        logger.info("No papers to report")
        return {"report_path": None}
    
    Config.REPORT_DIR.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Config.REPORT_DIR / f"research_report_{timestamp}.xlsx"
    
    wb = Workbook()
    
    # ==================
    # SHEET 1: Summary
    # ==================
    ws_summary = wb.active
    ws_summary.title = "Summary"
    
    # Title
    ws_summary['A1'] = "Research Monitoring Report"
    ws_summary['A1'].font = Font(size=16, bold=True, color="FFFFFF")
    ws_summary['A1'].fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    ws_summary.merge_cells('A1:B1')
    
    # Stats
    ws_summary.append([])
    ws_summary.append(["Report Date:", datetime.now().strftime("%Y-%m-%d %H:%M")])
    ws_summary.append(["Query:", filters.get("query", "N/A")])
    ws_summary.append([])
    ws_summary.append(["Statistics", ""])
    ws_summary.append(["Total Papers Fetched:", stats.get("total_fetched", 0)])
    ws_summary.append(["New Papers:", stats.get("new_papers", 0)])
    ws_summary.append(["After Filters:", stats.get("after_filters", 0)])
    ws_summary.append(["Filtered Out:", stats.get("filtered_out", 0)])
    
    # Active filters
    ws_summary.append([])
    ws_summary.append(["Active Filters", ""])
    
    if filters.get("author_names"):
        ws_summary.append(["Authors:", ", ".join(filters["author_names"])])
    if filters.get("author_institutions"):
        ws_summary.append(["Institutions:", ", ".join(filters["author_institutions"])])
    if filters.get("venues"):
        ws_summary.append(["Venues:", ", ".join(filters["venues"])])
    if filters.get("min_venue_rank"):
        ws_summary.append(["Min Venue Rank:", filters["min_venue_rank"]])
    if filters.get("min_citations"):
        ws_summary.append(["Min Citations:", filters["min_citations"]])
    if filters.get("min_year") or filters.get("max_year"):
        ws_summary.append(["Year Range:", f"{filters.get('min_year', 'N/A')} - {filters.get('max_year', 'N/A')}"])
    
    # Venue breakdown
    ws_summary.append([])
    ws_summary.append(["Venue Rank Distribution", "Count"])
    
    rank_counts = {}
    for paper in papers:
        rank = paper["venue_rank"]
        rank_counts[rank] = rank_counts.get(rank, 0) + 1
    
    for rank in ["A*", "Q1", "A", "B", "Unranked"]:
        if rank in rank_counts:
            ws_summary.append([rank, rank_counts[rank]])
    
    ws_summary.column_dimensions['A'].width = 30
    ws_summary.column_dimensions['B'].width = 40
    
    # ==================
    # SHEET 2: Papers
    # ==================
    ws_papers = wb.create_sheet("Papers")
    
    headers = ["#", "Title", "Authors", "Venue", "Rank", "Year", "Citations", "Pub Date"]
    ws_papers.append(headers)
    
    # Style headers
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for col in range(1, len(headers) + 1):
        cell = ws_papers.cell(1, col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border
    
    # Add papers
    for i, paper in enumerate(papers, 1):
        authors_str = ", ".join(paper.get("authors", [])[:3])
        if len(paper.get("authors", [])) > 3:
            authors_str += " et al."
        
        row = [
            i,
            paper["title"],
            authors_str,
            paper.get("venue", "N/A"),
            paper["venue_rank"],
            paper.get("year", "N/A"),
            paper["citations"],
            paper.get("publication_date", "N/A")
        ]
        ws_papers.append(row)
        
        # Apply borders
        for col in range(1, len(row) + 1):
            ws_papers.cell(i + 1, col).border = thin_border
    
    # Adjust widths
    ws_papers.column_dimensions['A'].width = 5
    ws_papers.column_dimensions['B'].width = 60
    ws_papers.column_dimensions['C'].width = 30
    ws_papers.column_dimensions['D'].width = 25
    ws_papers.column_dimensions['E'].width = 10
    ws_papers.column_dimensions['F'].width = 8
    ws_papers.column_dimensions['G'].width = 12
    ws_papers.column_dimensions['H'].width = 12
    
    # ==================
    # SHEET 3: Authors (if enriched)
    # ==================
    if any(p.get("author_details") for p in papers):
        ws_authors = wb.create_sheet("Author Details")
        
        author_headers = ["Paper", "Author", "Institution", "h-index", "Papers", "Citations"]
        ws_authors.append(author_headers)
        
        for col in range(1, len(author_headers) + 1):
            cell = ws_authors.cell(1, col)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")
        
        for paper in papers:
            for author in paper.get("author_details", []):
                ws_authors.append([
                    paper["title"][:40] + "...",
                    author["name"],
                    author["affiliation"],
                    author["h_index"],
                    author["paper_count"],
                    author["citations"]
                ])
        
        ws_authors.column_dimensions['A'].width = 45
        ws_authors.column_dimensions['B'].width = 25
        ws_authors.column_dimensions['C'].width = 35
        ws_authors.column_dimensions['D'].width = 10
        ws_authors.column_dimensions['E'].width = 10
        ws_authors.column_dimensions['F'].width = 12
    
    # Save
    wb.save(report_path)
    
    logger.info(f"✅ Report saved: {report_path}")
    return {"report_path": str(report_path)}


def send_email_node(state: ResearchState) -> ResearchState:
    """
    Agent 8: Send email notification
    """
    logger.info("📧 Agent 8: Sending email notification...")
    
    papers = state["filtered_papers"]
    report_path = state.get("report_path")
    stats = state.get("stats", {})
    filters = state["filters"]
    
    if not papers:
        logger.info("No papers - skipping email")
        return {"email_sent": False}
    
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = Config.EMAIL_FROM
        msg['To'] = ", ".join(Config.EMAIL_TO)
        msg['Subject'] = f"📚 {len(papers)} New Research Papers - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Rank breakdown
        rank_counts = {}
        for paper in papers:
            rank = paper["venue_rank"]
            rank_counts[rank] = rank_counts.get(rank, 0) + 1
        
        # Build HTML
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #4472C4; color: white; padding: 15px; }}
                .stats {{ background-color: #f5f5f5; padding: 10px; margin: 10px 0; }}
                .paper {{ margin: 15px 0; padding: 10px; border-left: 3px solid #4472C4; }}
                .badge {{ 
                    display: inline-block; 
                    padding: 3px 8px; 
                    border-radius: 3px; 
                    font-size: 11px; 
                    font-weight: bold;
                }}
                .rank-as {{ background-color: #28a745; color: white; }}
                .rank-q1 {{ background-color: #17a2b8; color: white; }}
                .rank-a {{ background-color: #ffc107; color: black; }}
                .rank-b {{ background-color: #6c757d; color: white; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🔬 Research Monitoring Report</h2>
                <p>{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
            </div>
            
            <div class="stats">
                <h3>Summary</h3>
                <ul>
                    <li><strong>Query:</strong> {filters.get('query', 'N/A')}</li>
                    <li><strong>New Papers Found:</strong> {stats.get('new_papers', 0)}</li>
                    <li><strong>Papers After Filters:</strong> {len(papers)}</li>
                </ul>
                
                <h4>Venue Rank Distribution:</h4>
                <ul>
        """
        
        for rank in ["A*", "Q1", "A", "B", "Unranked"]:
            if rank in rank_counts:
                html_body += f"<li><strong>{rank}:</strong> {rank_counts[rank]} papers</li>"
        
        html_body += """
                </ul>
            </div>
            
            <h3>Top Papers:</h3>
        """
        
        # Show top 10 papers
        for i, paper in enumerate(papers[:10], 1):
            rank = paper["venue_rank"]
            rank_class = {
                "A*": "rank-as",
                "Q1": "rank-q1",
                "A": "rank-a",
                "B": "rank-b"
            }.get(rank, "")
            
            authors = ", ".join(paper.get("authors", [])[:3])
            if len(paper.get("authors", [])) > 3:
                authors += " et al."
            
            html_body += f"""
            <div class="paper">
                <h4>{i}. {paper['title']}</h4>
                <p>
                    <strong>Authors:</strong> {authors}<br>
                    <strong>Venue:</strong> {paper.get('venue', 'N/A')} 
                    <span class="badge {rank_class}">{rank}</span><br>
                    <strong>Year:</strong> {paper.get('year', 'N/A')} | 
                    <strong>Citations:</strong> {paper['citations']}
                </p>
            </div>
            """
        
        if len(papers) > 10:
            html_body += f"<p><em>...and {len(papers) - 10} more papers in the attached Excel report.</em></p>"
        
        # Active filters
        if any([filters.get("author_names"), filters.get("author_institutions"), 
                filters.get("venues"), filters.get("min_venue_rank")]):
            html_body += """
            <div class="stats">
                <h4>Active Filters:</h4>
                <ul>
            """
            
            if filters.get("author_names"):
                html_body += f"<li><strong>Authors:</strong> {', '.join(filters['author_names'])}</li>"
            if filters.get("author_institutions"):
                html_body += f"<li><strong>Institutions:</strong> {', '.join(filters['author_institutions'])}</li>"
            if filters.get("venues"):
                html_body += f"<li><strong>Venues:</strong> {', '.join(filters['venues'])}</li>"
            if filters.get("min_venue_rank"):
                html_body += f"<li><strong>Min Rank:</strong> {filters['min_venue_rank']}</li>"
            
            html_body += "</ul></div>"
        
        html_body += """
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                This is an automated notification from the Research Monitoring System.
            </p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_body, 'html'))
        
        # Attach Excel
        if report_path and Path(report_path).exists():
            with open(report_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename={Path(report_path).name}'
                )
                msg.attach(part)
        
        # Send
        server = smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT)
        server.starttls()
        server.login(Config.EMAIL_FROM, Config.EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"✅ Email sent to {len(Config.EMAIL_TO)} recipients")
        return {"email_sent": True}
        
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        return {"email_sent": False, "error": str(e)}


# ================================
# CONDITIONAL ROUTING
# ================================
def should_continue_after_fetch(state: ResearchState) -> str:
    if state.get("error"):
        return "error"
    if not state.get("papers"):
        return "end"
    return "continue"


def has_new_papers(state: ResearchState) -> str:
    if state.get("new_papers"):
        return "process"
    logger.info("No new papers found")
    return "skip"


def has_filtered_papers(state: ResearchState) -> str:
    if state.get("filtered_papers"):
        return "continue"
    logger.info("No papers passed filters")
    return "end"


# ================================
# GRAPH CONSTRUCTION
# ================================
def build_workflow():
    """
    Multi-Agent Workflow with Advanced Filtering
    
    START
      ↓
    Agent 1: Fetch Papers
      ↓
    Agent 2: Filter Duplicates → [none] → END
      ↓
    Agent 3: Enrich Authors (if needed)
      ↓
    Agent 4: Apply User Filters → [none passed] → END
      ↓
    Agent 5: Analyze Impact
      ↓
    Agent 6: Store Data
      ↓
    Agent 7: Generate Excel
      ↓
    Agent 8: Send Email
      ↓
    END
    """
    workflow = StateGraph(ResearchState)
    
    # Add nodes
    workflow.add_node("fetch", fetch_papers_node)
    workflow.add_node("filter_duplicates", filter_duplicates_node)
    workflow.add_node("enrich_authors", enrich_authors_node)
    workflow.add_node("apply_filters", apply_filters_node)
    workflow.add_node("analyze_impact", analyze_impact_node)
    workflow.add_node("store", store_data_node)
    workflow.add_node("excel", generate_excel_node)
    workflow.add_node("email", send_email_node)
    
    # Define flow
    workflow.set_entry_point("fetch")
    
    workflow.add_conditional_edges(
        "fetch",
        should_continue_after_fetch,
        {"continue": "filter_duplicates", "end": END, "error": END}
    )
    
    workflow.add_conditional_edges(
        "filter_duplicates",
        has_new_papers,
        {"process": "enrich_authors", "skip": END}
    )
    
    workflow.add_edge("enrich_authors", "apply_filters")
    
    workflow.add_conditional_edges(
        "apply_filters",
        has_filtered_papers,
        {"continue": "analyze_impact", "end": END}
    )
    
    workflow.add_edge("analyze_impact", "store")
    workflow.add_edge("store", "excel")
    workflow.add_edge("excel", "email")
    workflow.add_edge("email", END)
    
    return workflow.compile()


# ================================
# MAIN EXECUTION
# ================================
class ResearchMonitor:
    def __init__(self):
        self.graph = build_workflow()
    
    def run(self):
        logger.info("=" * 70)
        logger.info("Starting Multi-Agent Research Monitoring Pipeline")
        logger.info("=" * 70)
        
        # Load filters
        filters = UserFilters.load()
        
        initial_state = {
            "filters": filters,
            "papers": [],
            "new_papers": [],
            "filtered_papers": [],
            "report_path": None,
            "email_sent": False,
            "error": None,
            "stats": {},
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        final_state = self.graph.invoke(initial_state)
        
        logger.info("=" * 70)
        logger.info("Pipeline Complete")
        logger.info(f"Papers found: {final_state.get('stats', {}).get('after_filters', 0)}")
        logger.info(f"Report: {final_state.get('report_path', 'N/A')}")
        logger.info(f"Email sent: {final_state.get('email_sent', False)}")
        logger.info("=" * 70)


def configure_filters_interactive():
    """Interactive CLI to configure filters"""
    print("\n" + "=" * 70)
    print("🔧 CONFIGURE RESEARCH FILTERS")
    print("=" * 70)
    
    filters = UserFilters.load()
    
    print("\nCurrent filters:")
    print(json.dumps(filters, indent=2))
    
    print("\n" + "-" * 70)
    print("Configure new filters (press Enter to skip)")
    print("-" * 70)
    
    # Query
    query = input(f"\nSearch query [{filters.get('query')}]: ").strip()
    if query:
        filters["query"] = query
    
    # Author names
    authors = input("\nAuthor names (comma-separated): ").strip()
    if authors:
        filters["author_names"] = [a.strip() for a in authors.split(",")]
    
    # Institutions
    institutions = input("\nInstitutions (comma-separated, e.g., MIT, Stanford, Google): ").strip()
    if institutions:
        filters["author_institutions"] = [i.strip() for i in institutions.split(",")]
    
    # Venues
    venues = input("\nVenues (comma-separated, e.g., CVPR, ICCV, NeurIPS): ").strip()
    if venues:
        filters["venues"] = [v.strip() for v in venues.split(",")]
    
    # Min venue rank
    print("\nMin venue rank options: A*, Q1, A, B")
    min_rank = input("Min venue rank (leave empty for any): ").strip()
    if min_rank:
        filters["min_venue_rank"] = min_rank
    
    # Citations
    min_cit = input("\nMin citations (0 for any): ").strip()
    if min_cit.isdigit():
        filters["min_citations"] = int(min_cit)
    
    # Years
    min_year = input("\nMin year (e.g., 2020): ").strip()
    if min_year.isdigit():
        filters["min_year"] = int(min_year)
    
    max_year = input("Max year (e.g., 2024): ").strip()
    if max_year.isdigit():
        filters["max_year"] = int(max_year)
    
    # Last N days
    last_days = input("\nPapers from last N days (leave empty to disable): ").strip()
    if last_days.isdigit():
        filters["last_n_days"] = int(last_days)
    
    # Keywords include
    keywords_inc = input("\nKeywords that MUST appear (comma-separated): ").strip()
    if keywords_inc:
        filters["keywords_include"] = [k.strip() for k in keywords_inc.split(",")]
    
    # Keywords exclude
    keywords_exc = input("\nKeywords to EXCLUDE (comma-separated): ").strip()
    if keywords_exc:
        filters["keywords_exclude"] = [k.strip() for k in keywords_exc.split(",")]
    
    # Max results
    max_res = input(f"\nMax results [{filters.get('max_results', 20)}]: ").strip()
    if max_res.isdigit():
        filters["max_results"] = int(max_res)
    
    # Save
    UserFilters.save(filters)
    
    print("\nFilters saved!")
    print("\nNew configuration:")
    print(json.dumps(filters, indent=2))


def main():
    import sys
    
    # Check for configuration command
    if len(sys.argv) > 1 and sys.argv[1] == "--configure":
        configure_filters_interactive()
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == "--run-once":
        monitor = ResearchMonitor()
        monitor.run()
        return
    
    # Normal scheduled operation
    monitor = ResearchMonitor()
    
    # Run immediately on startup
    logger.info("Running initial execution...")
    monitor.run()
    
    # Schedule daily runs
    scheduler = BlockingScheduler()
    scheduler.add_job(
        monitor.run, 
        'cron', 
        hour=Config.RUN_HOUR, 
        minute=Config.RUN_MINUTE,
        name='daily_research_monitor'
    )
    
    logger.info(f"Scheduler active - runs daily at {Config.RUN_HOUR:02d}:{Config.RUN_MINUTE:02d}")
    logger.info("Commands:")
    logger.info("   python script.py --configure   (set filters)")
    logger.info("   python script.py --run-once    (single run)")
    logger.info("   Press Ctrl+C to stop")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("\nShutdown")


if __name__ == "__main__":
    main()