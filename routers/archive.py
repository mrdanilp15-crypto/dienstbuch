from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

router = APIRouter(prefix="/api/archive", tags=["Dokumenten-Archiv"])

# --- ARCHIV: DOKUMENTE LISTEN ---
@router.get("/list")
def list_archive(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT id, title, keywords, file_blob, DATE_FORMAT(uploaded_at, '%d.%m.%Y %H:%i') as date_formatted FROM archive_docs ORDER BY id DESC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- ARCHIV: DOKUMENT HOCHLADEN ---
@router.post("/upload")
async def upload_archive_doc(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("INSERT INTO archive_docs (title, keywords, file_blob) VALUES (%s, %s, %s)", (d.get('title'), d.get('keywords'), d.get('file_blob')))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- ARCHIV: DOKUMENT LÖSCHEN ---
@router.delete("/{doc_id}")
def delete_archive_doc(doc_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM archive_docs WHERE id = %s", (doc_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))