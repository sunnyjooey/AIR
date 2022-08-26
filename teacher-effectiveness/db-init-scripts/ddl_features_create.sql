create table applause
(
    username        text     null,
    class_timestamp datetime null,
    start_time      double   null,
    end_time        double   null
);

create table emotion
(
    username        varchar(32)  null,
    class_timestamp datetime     null,
    frame_number    int          null,
    time            float        null,
    face_location   text         null,
    emotion         varchar(128) null,
    emotion_prob    float        null
);

create table file_locations
(
    username            varchar(32)  null,
    class_timestamp     datetime     null,
    subject             varchar(248) null,
    grade               varchar(248) null,
    date                datetime     null,
    video_location      varchar(248) null,
    audio_location      varchar(248) null,
    transcript_location varchar(248) null
);

create table insults
(
    username        varchar(32) null,
    class_timestamp datetime    null,
    sentence_index  int         null,
    insult_rating   float       null,
    foul_language   float       null
);

create table jargon
(
    username        varchar(32)  null,
    class_timestamp datetime     null,
    word            varchar(128) null,
    tf              float        null
);

create table laughter
(
    username        text     null,
    class_timestamp datetime null,
    filename        text     null,
    start_time      double   null,
    end_time        double   null
);

create table question
(
    username        text     null,
    class_timestamp datetime null,
    sentence_index  bigint   null,
    sentence_type   text     null
);

create table responsePauses
(
    username          varchar(32) null,
    class_timestamp   datetime    null,
    content           text        null,
    end_time          float       null,
    response_duration float       null,
    same_speaker      varchar(64) null,
    speaker_label     varchar(64) null,
    start_time        float       null,
    type              varchar(64) null
);

create table sentences
(
    username        varchar(32) null,
    class_timestamp datetime    null,
    sentence_index  int         null,
    punct_index     int         null,
    speaker_label   text        null,
    start_time      float       null,
    end_time        float       null,
    sentence        text        null
);

create table sentiment
(
    username           varchar(32) null,
    class_timestamp    datetime    null,
    sentence_index     int         null,
    text_blob_score    float       null,
    vader_polarity_neg float       null,
    vader_polarity_neu float       null,
    vader_polarity_pos float       null
);

create table speakertags
(
    username        varchar(32) null,
    class_timestamp datetime    null,
    speaker         text        null,
    start_time      float       null,
    end_time        float       null,
    speech          text        null
);

create table unofficial
(
    username        varchar(32)  null,
    class_timestamp datetime     null,
    word            varchar(128) null,
    tf              float        null
);

create table words
(
    username          varchar(32) null,
    class_timestamp   datetime    null,
    word_count        int         null,
    unique_word_count int         null,
    atleast_2_times   int         null,
    atleast_3_times   int         null,
    atleast_5_times   int         null,
    words             text        null
);

create table yelling
(
    username        varchar(32) null,
    class_timestamp datetime    null,
    speaker         text        null,
    start_time      float       null,
    end_time        float       null,
    speech          text        null,
    loudness_ratio  float       null,
    yelling         varchar(64) null
);

