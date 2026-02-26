CREATE TABLE IF NOT EXISTS groups_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS persons (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    group_id INT,
    FOREIGN KEY (group_id) REFERENCES groups_table(id)
);

CREATE TABLE IF NOT EXISTS sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    group_id INT,
    category ENUM('Übung', 'Einsatz', 'Sonstiges') DEFAULT 'Übung',
    duration FLOAT DEFAULT 0.0,
    description TEXT,
    instructors TEXT,
    leader_signature LONGTEXT,
    FOREIGN KEY (group_id) REFERENCES groups_table(id)
);

CREATE TABLE IF NOT EXISTS attendance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id INT,
    person_id INT,
    is_present TINYINT(1) DEFAULT 0,
    note TEXT,
    vehicle VARCHAR(50),
    signature LONGTEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(id),
    FOREIGN KEY (person_id) REFERENCES persons(id)
);
