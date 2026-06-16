import os
import mysql.connector

DB_PASSWORD = os.getenv("DB_PASSWORD", "feuerwehr")

def get_db_connection():
    """Baut die Verbindung zum MySQL/MariaDB-Container auf."""
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

def init_db():
    """Erstellt alle Tabellen und führt Migrationen durch."""
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Basistabellen anlegen
        cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL, birth_date DATE NULL, entry_date DATE NULL, phone VARCHAR(100) DEFAULT '', email VARCHAR(255) DEFAULT '', address TEXT NULL, ice_contact VARCHAR(255) DEFAULT '', drive_b BOOLEAN DEFAULT 0, drive_be BOOLEAN DEFAULT 0, drive_c BOOLEAN DEFAULT 0, drive_ce BOOLEAN DEFAULT 0, profile_picture LONGTEXT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL, next_oil_change_km INT DEFAULT 10000, license_plate VARCHAR(50) DEFAULT '', vehicle_type VARCHAR(100) DEFAULT '') ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicle_log (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT, date DATE, driver_name VARCHAR(255), purpose VARCHAR(255), km_start INT, km_end INT, fuel_liters FLOAT DEFAULT 0.0) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', qr_code_id VARCHAR(100) DEFAULT '', last_check DATE NULL, next_check DATE NULL, category VARCHAR(100) DEFAULT '', manufacturer VARCHAR(100) DEFAULT '', serial_number VARCHAR(100) DEFAULT '') ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS tickets (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), content TEXT, vehicle_id INT NULL, inventory_id INT NULL, priority VARCHAR(50) DEFAULT 'normal', status VARCHAR(50) DEFAULT 'neu', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS active_alarm (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), keyword VARCHAR(100), alert_text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS hydranten (id INT AUTO_INCREMENT PRIMARY KEY, lat DOUBLE, lon DOUBLE, hydrant_type VARCHAR(100), diameter VARCHAR(50), last_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS archive_docs (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), keywords TEXT, file_blob LONGTEXT, uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")

        # Standard-Einstellungen setzen, falls leer
        settings_defaults = [
            ('station_name', 'Freiwillige Feuerwehr'),
            ('station_lat', '47.9942'),
            ('station_lon', '10.1344')
        ]
        for k, v in settings_defaults:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))

        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        c.commit()
        cur.close()
        c.close()
        print("-> [Datenbank] Erfolgreich synchronisiert und einsatzbereit.")
    except Exception as e:
        print(f"-> [Datenbank] KRITISCHER FEHLER: {e}")