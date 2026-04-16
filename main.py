"""
Job Hunt Agent - Main Pipeline
Scrapes jobs via Apify, analyzes with Claude, stores in Supabase
"""

import os
import json
import time
import httpx
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────────
APIFY_TOKEN       = os.environ["APIFY_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_KEY"]

SEARCH_QUERIES = [
    "founder's office",
    "founders office generalist",
    "chief of staff",
    "CEO office generalist",
    "EIR entrepreneur in residence",
    "build with AI generalist",
    "0 to 1 generalist startup",
    "founding team generalist",
]

LOCATIONS = ["Bangalore", "Mumbai", "Delhi", "Dubai", "Remote India"]

HIMJA_PROFILE = """
Himja Behl — Founder's Office Operator & AI Builder

EXPERIENCE:
- Business Analyst, Founder's Office at Groyyo (B2B fashion tech, Aug 2023–Dec 2024)
  • Led ~$20M working capital recovery initiative end-to-end, managed 5-person team + 2 agencies
  • Owned full receivables lifecycle: MIS tracking, aging analysis, recovery strategy
  • Independently executed liquidation of dormant fashion inventory across 3 e-commerce platforms → $150K+ revenue unlocked
  • Pricing, channel optimisation, sell-through strategy across B2B and B2C brands
  • Participated in investor MIS discussions and financial reviews
- Business Intern at Purple Style Labs (Mumbai, 2022): ROIC modelling, MENA localisation (2.5x traffic), campaign design
- Investment Analyst Intern at Singapore Angel Network: Evaluated 50+ startups, built investment thesis

AI BUILDING (non-engineer):
- Built WOW (What. Outfit. When.) — AI-powered personal styling app with taste-learning engine, AI APIs, deployed on Vercel/Render
- Built Mosaic invoice audit intelligence tool (vanilla JS, Chart.js, Vercel) that identified systematic GST overcharging
- Built Kitna — AI-powered budgeting tool
- Has been building with AI tools for months without a formal engineering background

EDUCATION: BMS, St. Xavier's College Mumbai — 9.6 CGPA

STRENGTHS: Working capital & receivables, marketplace operations, first-principles execution, high agency, comfort with ambiguity, AI-native operator, builds real products without being an engineer

LOCATION: Bangalore (open to Mumbai, Delhi, Dubai)
"""

# ── Supabase helpers ─────────────────────────────────────────────────────────
def supabase_insert(records: list[dict]) -> dict:
    url = f"{SUPABASE_URL}/rest/v1/jobs"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    r = httpx.post(url, headers=headers, json=records, timeout=30)
    return r.json() if r.text else {}

def supabase_get_existing_urls() -> set:
    url = f"{SUPABASE_URL}/rest/v1/jobs?select=job_url"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = httpx.get(url, headers=headers, timeout=30)
    data = r.json()
    return {row["job_url"] for row in data if row.get("job_url")}

def supabase_update(job_id: str, data: dict):
    url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    httpx.patch(url, headers=headers, json=data, timeout=30)

def supabase_get_unanalyzed() -> list[dict]:
    url = f"{SUPABASE_URL}/rest/v1/jobs?analysis_done=eq.false&select=*&limit=20"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = httpx.get(url, headers=headers, timeout=30)
    return r.json()

# ── Apify scraper ─────────────────────────────────────────────────────────────
def run_apify_actor(actor_id: str, input_data: dict) -> list[dict]:
    """Run an Apify actor and return results."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    
    # Start run
    r = httpx.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        headers=headers,
        json=input_data,
        timeout=60,
    )
    run_data = r.json()
    run_id = run_data.get("data", {}).get("id")
    if not run_id:
        print(f"  Failed to start actor: {run_data}")
        return []
    
    # Poll until done
    for _ in range(60):
        time.sleep(10)
        status_r = httpx.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers, timeout=30)
        status = status_r.json().get("data", {}).get("status")
        print(f"  Run status: {status}")
        if status in ("SUCCEEDED", "FAILED", "ABORTED"):
            break
    
    if status != "SUCCEEDED":
        print(f"  Actor run did not succeed: {status}")
        return []
    
    # Get results
    dataset_id = status_r.json()["data"]["defaultDatasetId"]
    results_r = httpx.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?limit=100",
        headers=headers,
        timeout=60,
    )
    return results_r.json()

def scrape_linkedin_jobs() -> list[dict]:
    """Scrape LinkedIn jobs using Apify's LinkedIn Jobs Scraper."""
    print("Scraping LinkedIn...")
    jobs = []
    
    for query in SEARCH_QUERIES[:4]:  # Limit to avoid rate limits
        for location in LOCATIONS[:3]:
            print(f"  Query: {query} @ {location}")
            results = run_apify_actor(
                "curious_coder/linkedin-jobs-scraper",
                {
                    "searchQueries": [{"query": query, "location": location}],
                    "maxResults": 10,
                    "proxy": {"useApifyProxy": True},
                }
            )
            jobs.extend(results)
            time.sleep(2)
    
    return jobs

def scrape_wellfound_jobs() -> list[dict]:
    """Scrape Wellfound/AngelList jobs."""
    print("Scraping Wellfound...")
    results = run_apify_actor(
        "curious_coder/wellfound-scraper",
        {
            "searchKeywords": ["founder office", "chief of staff", "generalist"],
            "locations": ["india", "dubai"],
            "maxResults": 30,
        }
    )
    return results

def normalize_job(raw: dict, platform: str) -> dict | None:
    """Normalize raw job data from any platform into our schema."""
    title = raw.get("title") or raw.get("jobTitle") or raw.get("position") or ""
    company = raw.get("company") or raw.get("companyName") or raw.get("employer") or ""
    url = raw.get("url") or raw.get("jobUrl") or raw.get("link") or ""
    location = raw.get("location") or raw.get("jobLocation") or ""
    jd = raw.get("description") or raw.get("jobDescription") or raw.get("content") or ""
    
    if not title or not company or not url:
        return None
    
    return {
        "title": title[:500],
        "company": company[:200],
        "location": location[:200],
        "platform": platform,
        "job_url": url[:1000],
        "jd_text": jd[:5000],
        "posted_date": raw.get("postedDate") or raw.get("date") or "",
        "analysis_done": False,
        "status": "new",
    }

# ── Claude analyzer ───────────────────────────────────────────────────────────
def analyze_job_with_claude(job: dict) -> dict:
    """Use Claude to analyze fit and generate outreach for a job."""
    
    jd_snippet = (job.get("jd_text") or "")[:3000]
    
    prompt = f"""You are analyzing a job opportunity for Himja Behl.

CANDIDATE PROFILE:
{HIMJA_PROFILE}

JOB DETAILS:
Title: {job['title']}
Company: {job['company']}
Location: {job.get('location', 'Unknown')}
Platform: {job.get('platform', 'Unknown')}

JOB DESCRIPTION:
{jd_snippet if jd_snippet else "No JD available - analyze based on title and company only"}

Your task: Analyze fit and generate outreach. Respond ONLY with a valid JSON object, no markdown, no preamble.

{{
  "fit_score": <integer 1-10, where 10 = perfect match>,
  "company_summary": "<2 sentence summary of what the company does and stage>",
  "company_stage": "<one of: early-stage, growth-stage, scale-up, unknown>",
  "fit_reasons": ["<reason 1>", "<reason 2>", "<reason 3>"],
  "gaps": ["<gap 1 if any>"],
  "draft_outreach": "<A 4-6 line LinkedIn DM or email to the founder/hiring manager. Personalized, specific, not generic. Reference Himja's working capital recovery, AI building, or relevant experience that matches THIS specific role. Confident but not salesy. End with a clear ask.>",
  "founder_search_hint": "<What to search on LinkedIn to find the founder - e.g. 'CEO of CompanyName' or founder name if known>"
}}"""

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": prompt}],
    }
    
    r = httpx.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=60)
    data = r.json()
    
    text = data["content"][0]["text"].strip()
    # Strip any markdown fences
    text = text.replace("```json", "").replace("```", "").strip()
    
    parsed = json.loads(text)
    return parsed

# ── Main pipeline ─────────────────────────────────────────────────────────────
def run_scrape():
    """Scrape new jobs and store in Supabase."""
    print(f"\n{'='*50}")
    print(f"Job Hunt Agent — Scrape Run: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*50}")
    
    existing_urls = supabase_get_existing_urls()
    print(f"Already have {len(existing_urls)} jobs in DB")
    
    all_raw = []
    
    try:
        linkedin_jobs = scrape_linkedin_jobs()
        all_raw.extend([(j, "linkedin") for j in linkedin_jobs])
        print(f"LinkedIn: {len(linkedin_jobs)} raw results")
    except Exception as e:
        print(f"LinkedIn scrape failed: {e}")
    
    try:
        wf_jobs = scrape_wellfound_jobs()
        all_raw.extend([(j, "wellfound") for j in wf_jobs])
        print(f"Wellfound: {len(wf_jobs)} raw results")
    except Exception as e:
        print(f"Wellfound scrape failed: {e}")
    
    # Normalize + deduplicate
    new_jobs = []
    for raw, platform in all_raw:
        normalized = normalize_job(raw, platform)
        if normalized and normalized["job_url"] not in existing_urls:
            new_jobs.append(normalized)
            existing_urls.add(normalized["job_url"])
    
    print(f"\nNew jobs to insert: {len(new_jobs)}")
    
    if new_jobs:
        supabase_insert(new_jobs)
        print(f"Inserted {len(new_jobs)} jobs into Supabase")
    
    return len(new_jobs)

def run_analyze():
    """Analyze unprocessed jobs with Claude."""
    print(f"\nAnalyzing unprocessed jobs...")
    
    jobs = supabase_get_unanalyzed()
    print(f"Found {len(jobs)} jobs to analyze")
    
    analyzed = 0
    for job in jobs:
        try:
            print(f"  Analyzing: {job['title']} @ {job['company']}")
            analysis = analyze_job_with_claude(job)
            
            supabase_update(job["id"], {
                "fit_score": analysis.get("fit_score"),
                "company_summary": analysis.get("company_summary"),
                "company_stage": analysis.get("company_stage"),
                "fit_reasons": analysis.get("fit_reasons", []),
                "gaps": analysis.get("gaps", []),
                "draft_outreach": analysis.get("draft_outreach"),
                "founder_linkedin": analysis.get("founder_search_hint"),
                "analysis_done": True,
            })
            analyzed += 1
            time.sleep(1)  # Rate limit
        except Exception as e:
            print(f"  Error analyzing {job.get('title')}: {e}")
            # Mark as done anyway to avoid infinite retry
            supabase_update(job["id"], {"analysis_done": True})
    
    print(f"Analyzed {analyzed} jobs")
    return analyzed

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"
    
    if mode in ("scrape", "both"):
        run_scrape()
    if mode in ("analyze", "both"):
        run_analyze()
    
    print("\nDone!")
