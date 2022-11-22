CREATE TABLE aka_name
(
    id            int,
    person_id     int,
    name          string(100),
    imdb_index    string(12),
    name_pcode_cf string(5),
    name_pcode_nf string(5),
    surname_pcode string(5),
    md5sum        string(32)
);

CREATE TABLE aka_title
(
    id              int,
    movie_id        int,
    title           string(100),
    imdb_index      string(12),
    kind_id         int,
    production_year int,
    phonetic_code   string(5),
    episode_of_id   int,
    season_nr       int,
    episode_nr      int,
    note            string(100),
    md5sum          string(32)
);

CREATE TABLE cast_info
(
    id             int,
    person_id      int,
    movie_id       int,
    person_role_id int,
    note           string(100),
    nr_order       int,
    role_id        int
);

CREATE TABLE char_name
(
    id            int,
    name          string(100),
    imdb_index    string(12),
    imdb_id       int,
    name_pcode_nf string(5),
    surname_pcode string(5),
    md5sum        string(32)
);

CREATE TABLE comp_cast_type
(
    id   int,
    kind string(32)
);

CREATE TABLE company_name
(
    id            int,
    name          string(100),
    country_code  string(255),
    imdb_id       int,
    name_pcode_nf string(5),
    name_pcode_sf string(5),
    md5sum        string(32)
);

CREATE TABLE company_type
(
    id   int,
    kind string(32)
);

CREATE TABLE complete_cast
(
    id         int,
    movie_id   int,
    subject_id int,
    status_id  int
);

CREATE TABLE info_type
(
    id   int,
    info string(32)
);

CREATE TABLE keyword
(
    id            int,
    keyword       string(100),
    phonetic_code string(5)
);

CREATE TABLE kind_type
(
    id   int,
    kind string(15)
);

CREATE TABLE link_type
(
    id   int,
    link string(32)
);

CREATE TABLE movie_companies
(
    id              int,
    movie_id        int,
    company_id      int,
    company_type_id int,
    note            string(100)
);

CREATE TABLE movie_info
(
    id           int,
    movie_id     int,
    info_type_id int,
    info         string(100),
    note         string(100)
);

CREATE TABLE movie_info_idx
(
    id           int,
    movie_id     int,
    info_type_id int,
    info         string(100),
    note         string(100)
);

CREATE TABLE movie_keyword
(
    id         int,
    movie_id   int,
    keyword_id int
);

CREATE TABLE movie_link
(
    id              int,
    movie_id        int,
    linked_movie_id int,
    link_type_id    int
);

CREATE TABLE name
(
    id            int,
    name          string(100),
    imdb_index    string(12),
    imdb_id       int,
    gender        string(1),
    name_pcode_cf string(5),
    name_pcode_nf string(5),
    surname_pcode string(5),
    md5sum        string(32)
);

CREATE TABLE person_info
(
    id           int,
    person_id    int,
    info_type_id int,
    info         string(100),
    note         string(100)
);

CREATE TABLE role_type
(
    id   int,
    role string(32)
);

CREATE TABLE title
(
    id              int,
    title           string(100),
    imdb_index      string(12),
    kind_id         int,
    production_year int,
    imdb_id         int,
    phonetic_code   string(5),
    episode_of_id   int,
    season_nr       int,
    episode_nr      int,
    series_years    string(49),
    md5sum          string(32)
);