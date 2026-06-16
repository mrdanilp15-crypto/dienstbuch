import os
import mysql.connector
import time
import hashlib
import secrets

DB_PASSWORD = os.getenv("DB_PASSWORD", "feuerwehr")

def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

def hash_password(p: str) -> str:
    s = secrets.token_hex(16)
    return f"{s}:{hashlib.pbkdf2_hmac('sha256', p.encode(), s.encode(), 100000).hex()}"

def init_db():
    print("-> [Datenbank] Starte strukturelle Initialisierung...")
    c = None
    
    for i in range(12):
        try:
            c = get_db_connection()
            print("-> [Datenbank] Verbindung zu Port 3306 erfolgreich hergestellt.")
            break
        except Exception:
            print(f"-> [Datenbank] MariaDB konfiguriert sich noch... Warte auf Handshake (Versuch {i+1}/12)...")
            time.sleep(3)
    
    if not c:
        print("-> [Datenbank] KRITISCHER FEHLER: MariaDB konnte nicht erreicht werden!")
        return

    try:
        cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Basistabellen erzeugen
        cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL, birth_date DATE NULL, entry_date DATE NULL, phone VARCHAR(100) DEFAULT '', email VARCHAR(255) DEFAULT '', address TEXT NULL, ice_contact VARCHAR(255) DEFAULT '', drive_b BOOLEAN DEFAULT 0, drive_be BOOLEAN DEFAULT 0, drive_c BOOLEAN DEFAULT 0, drive_ce BOOLEAN DEFAULT 0, profile_picture LONGTEXT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL, next_oil_change_km INT DEFAULT 10000, license_plate VARCHAR(50) DEFAULT '', vehicle_type VARCHAR(100) DEFAULT '') ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicle_log (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT, date DATE, driver_name VARCHAR(255), purpose VARCHAR(255), km_start INT, km_end INT, fuel_liters FLOAT DEFAULT 0.0) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', qr_code_id VARCHAR(100) DEFAULT '', last_check DATE NULL, next_check DATE NULL, category VARCHAR(100) DEFAULT '', manufacturer VARCHAR(100) DEFAULT '', serial_number VARCHAR(100) DEFAULT '', size VARCHAR(50) DEFAULT '') ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS tickets (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), content TEXT, vehicle_id INT NULL, inventory_id INT NULL, priority VARCHAR(50) DEFAULT 'normal', status VARCHAR(50) DEFAULT 'neu', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS active_alarm (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), keyword VARCHAR(100), alert_text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS hydranten (id INT AUTO_INCREMENT PRIMARY KEY, lat DOUBLE, lon DOUBLE, hydrant_type VARCHAR(100), diameter VARCHAR(50), last_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS archive_docs (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), keywords TEXT, file_blob LONGTEXT, uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")

        # PSA Zuordnungstabelle
        cur.execute("""
            CREATE TABLE IF NOT EXISTS psa (
                id INT AUTO_INCREMENT PRIMARY KEY,
                person_id INT NOT NULL,
                item_name VARCHAR(255) NOT NULL,
                size VARCHAR(50) DEFAULT '',
                qr_code_id VARCHAR(100) DEFAULT '',
                issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        # Struktur-Updates nachziehen
        migrations = [
            ("tickets", "created_by", "INT NULL"),
            ("vehicles", "operating_hours", "FLOAT DEFAULT 0.0"),
            ("personnel", "qualifications", "TEXT NULL")
            ("personnel", "size_helm", "VARCHAR(50) DEFAULT ''"),   # NEU
            ("personnel", "size_jacke", "VARCHAR(50) DEFAULT ''"),  # NEU
            ("personnel", "size_stiefel", "VARCHAR(50) DEFAULT ''") # NEU
        ]
        for table, column, definition in migrations:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition};")
                print(f"-> [Migration] Spalte '{column}' in '{table}' erfolgreich geprüft/nachgezogen.")
            except:
                pass

        settings_defaults = [
            ('station_name', 'Freiwillige Feuerwehr Buxheim'),
            ('station_lat', '47.9994'),
            ('station_lon', '10.1325')
        ]
        for k, v in settings_defaults:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))

        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)", ("admin", hash_password("admin123"), "admin"))

        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        c.commit()
        cur.close()
        c.close()
        print("-> [Datenbank] Alle Strukturen und PSA-Tabellen erfolgreich geladen.")
    except Exception as e:
        print(f"-> [Datenbank] KRITISCHER FEHLER beim Tabellenaufbau: {e}")