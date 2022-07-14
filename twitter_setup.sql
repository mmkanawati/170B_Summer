CREATE TABLE playrates (
    "date" DATE,
    "champion" TEXT,
    "playrate" DOUBLE PRECISION
);
CREATE TABLE twitter_champions (
    "id" bigint,
    "topic_champion" TEXT
);
CREATE TABLE t_table (
    "t975" double precision
);
CREATE TABLE twitter_champions_arrays (
    "id" bigint,
    "text" TEXT,
    "topic_champion" TEXT
);
CREATE TABLE twitter_vectors (
    "id" bigint,
    "0" double precision, "1" double precision, "2" double precision, "3" double precision, "4" double precision, "5" double precision, "6" double precision, "7" double precision, "8" double precision, "9" double precision, "10" double precision,  "11" double precision, "12" double precision, "13" double precision, "14" double precision, "15" double precision, "16" double precision, "17" double precision, "18" double precision, "19" double precision, "20" double precision, "21" double precision, "22" double precision, "23" double precision, "24" double precision, "25" double precision, "26" double precision, "27" double precision, "28" double precision, "29" double precision, "30" double precision, "31" double precision, "32" double precision, "33" double precision, "34" double precision, "35" double precision, "36" double precision, "37" double precision, "38" double precision, "39" double precision, "40" double precision, "41" double precision, "42" double precision, "43" double precision, "44" double precision, "45" double precision, "46" double precision, "47" double precision, "48" double precision, "49" double precision, "50" double precision, "51" double precision, "52" double precision, "53" double precision, "54" double precision, "55" double precision, "56" double precision, "57" double precision, "58" double precision, "59" double precision, "60" double precision, "61" double precision, "62" double precision, "63" double precision, "64" double precision, "65" double precision, "66" double precision, "67" double precision, "68" double precision, "69" double precision, "70" double precision, "71" double precision, "72" double precision, "73" double precision, "74" double precision, "75" double precision, "76" double precision, "77" double precision, "78" double precision, "79" double precision, "80" double precision, "81" double precision, "82" double precision, "83" double precision, "84" double precision, "85" double precision, "86" double precision, "87" double precision, "88" double precision, "89" double precision, "90" double precision, "91" double precision, "92" double precision, "93" double precision, "94" double precision, "95" double precision, "96" double precision, "97" double precision, "98" double precision, "99" double precision
);
CREATE TABLE twitter (
    "id" bigint,
    "text" TEXT,
    "create_at" date,
    "followers_count" double precision
);


ALTER TABLE twitter 
ALTER COLUMN "create_at" TYPE date USING create_at::date;


------------------------------------------------------------------------------------------------------------------

CREATE FUNCTION t_val(n integer) 
RETURNS double precision
LANGUAGE SQL
AS 
$$
select x.tv as t_value FROM (
    select ROW_NUMBER() OVER (ORDER BY "t975" DESC) AS row_num,
        "t975" as tv
    FROM t_table) as x
WHERE x.row_num=n;
$$
;

CREATE FUNCTION t_sample_t_test(champ text, ymd date) 
RETURNS boolean
LANGUAGE SQL
AS 
$$
SELECT e.tvalue > t_val(e.t_::int) as signficance
    FROM (SELECT (d.post_avg - d.pre_avg) / SQRT((d.var_samp_pre/d.pre_count) + (d.var_samp_post/d.post_count)) as tvalue,
    ABS(d.pre_count + d.post_count -2) as t_
        FROM
        (SELECT pr.avgpr as pre_avg,
		 po.avgpo as post_avg,
        pr.cntpr pre_count,
        po.cntpo as post_count,
        pr.vspr as var_samp_pre,
        po.vspo as var_samp_post
            FROM
             (SELECT COUNT("date") cntpr, var_samp(playrate) vspr, avg(playrate) avgpr
                FROM playrates
                WHERE "date" > ymd::date - INTERVAL '7 day'
                AND "date" <= ymd::date
                AND "champion" = champ)
        AS pr,
            (SELECT COUNT("date") cntpo, var_samp(playrate) vspo, avg(playrate) avgpo
                FROM playrates
                WHERE "date" > ymd::date
                AND "date" <= ymd::date + INTERVAL '7 day'
                AND "champion" = champ)
		as po)
    AS d)
AS e;
$$;

CREATE OR REPLACE FUNCTION model_parameters ()
	RETURNS TABLE (
		"id" bigint,
		"create_at" date,
		"followers_count" double precision,
		"0" double precision,
		"1" double precision,
		"2" double precision,
		"3" double precision,
		"4" double precision,
		"5" double precision,
		"6" double precision,
		"7" double precision,
		"8" double precision,
		"9" double precision,
		"10" double precision,
		"11" double precision,
		"12" double precision,
		"13" double precision,
		"14" double precision,
		"15" double precision,
		"16" double precision,
		"17" double precision,
		"18" double precision,
		"19" double precision,
		"20" double precision,
		"21" double precision,
		"22" double precision,
		"23" double precision,
		"24" double precision,
		"25" double precision,
		"26" double precision,
		"27" double precision,
		"28" double precision,
		"29" double precision,
		"30" double precision,
		"31" double precision,
		"32" double precision,
		"33" double precision,
		"34" double precision,
		"35" double precision,
		"36" double precision,
		"37" double precision,
		"38" double precision,
		"39" double precision,
		"40" double precision,
		"41" double precision,
		"42" double precision,
		"43" double precision,
		"44" double precision,
		"45" double precision,
		"46" double precision,
		"47" double precision,
		"48" double precision,
		"49" double precision,
		"50" double precision,
		"51" double precision,
		"52" double precision,
		"53" double precision,
		"54" double precision,
		"55" double precision,
		"56" double precision,
		"57" double precision,
		"58" double precision,
		"59" double precision,
		"60" double precision,
		"61" double precision,
		"62" double precision,
		"63" double precision,
		"64" double precision,
		"65" double precision,
		"66" double precision,
		"67" double precision,
		"68" double precision,
		"69" double precision,
		"70" double precision,
		"71" double precision,
		"72" double precision,
		"73" double precision,
		"74" double precision,
		"75" double precision,
		"76" double precision,
		"77" double precision,
		"78" double precision,
		"79" double precision,
		"80" double precision,
		"81" double precision,
		"82" double precision,
		"83" double precision,
		"84" double precision,
		"85" double precision,
		"86" double precision,
		"87" double precision,
		"88" double precision,
		"89" double precision,
		"90" double precision,
		"91" double precision,
		"92" double precision,
		"93" double precision,
		"94" double precision,
		"95" double precision,
		"96" double precision,
		"97" double precision,
		"98" double precision,
		"99" double precision
)
AS $$
BEGIN
	RETURN TABLE
	select s."id",
	s."create_at",
	s."followers_count",
	sv."0",
	sv."1",
	sv."2",
	sv."3",
	sv."4",
	sv."5",
	sv."6",
	sv."7",
	sv."8",
	sv."9",
	sv."10",
	sv."11",
	sv."12",
	sv."13",
	sv."14",
	sv."15",
	sv."16",
	sv."17",
	sv."18",
	sv."19",
	sv."20",
	sv."21",
	sv."22",
	sv."23",
	sv."24",
	sv."25",
	sv."26",
	sv."27",
	sv."28",
	sv."29",
	sv."30",
	sv."31",
	sv."32",
	sv."33",
	sv."34",
	sv."35",
	sv."36",
	sv."37",
	sv."38",
	sv."39",
	sv."40",
	sv."41",
	sv."42",
	sv."43",
	sv."44",
	sv."45",
	sv."46",
	sv."47",
	sv."48",
	sv."49",
	sv."50",
	sv."51",
	sv."52",
	sv."53",
	sv."54",
	sv."55",
	sv."56",
	sv."57",
	sv."58",
	sv."59",
	sv."60",
	sv."61",
	sv."62",
	sv."63",
	sv."64",
	sv."65",
	sv."66",
	sv."67",
	sv."68",
	sv."69",
	sv."70",
	sv."71",
	sv."72",
	sv."73",
	sv."74",
	sv."75",
	sv."76",
	sv."77",
	sv."78",
	sv."79",
	sv."80",
	sv."81",
	sv."82",
	sv."83",
	sv."84",
	sv."85",
	sv."86",
	sv."87",
	sv."88",
	sv."89",
	sv."90",
	sv."91",
	sv."92",
	sv."93",
	sv."94",
	sv."95",
	sv."96",
	sv."97",
	sv."98",
	sv."99"
	from twitter as s JOIN twitter_vectors as sv
	ON s."id" = sv."id";
END;
$$ LANGUAGE plpgsql;