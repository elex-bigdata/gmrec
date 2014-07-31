/*查看任务计划是否开启*/
SHOW VARIABLES LIKE '%sche%';

/*开启任务计划*/
SET GLOBAL event_scheduler =1;

/*存储过程开始*/
DELIMITER $$

CREATE
    /*[DEFINER = { user | CURRENT_USER }]*/
    PROCEDURE `test`.`tagLoader`()
    /*LANGUAGE SQL
    | [NOT] DETERMINISTIC
    | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
    | SQL SECURITY { DEFINER | INVOKER }
    | COMMENT 'string'*/
    
    BEGIN
        SELECT b.game_identify,GROUP_CONCAT(b.id SEPARATOR ':'),GROUP_CONCAT(b.tagname SEPARATOR ':') FROM(SELECT m.game_identify,t.id,t.tagname FROM www_tag t,www_tag_minigame_map m WHERE m.tag_id = t.id AND t.language = 'en' AND m.game_identify !='' ORDER BY m.game_identify)b GROUP BY b.game_identify INTO OUTFILE "D:\\tag.csv" FIELDS TERMINATED BY ',';
    END$$

DELIMITER ;

/*存储过程结束*/

/*设置定时任务开始*/

DELIMITER $$

-- SET GLOBAL event_scheduler = ON$$     -- required for event to execute but not create    

CREATE	/*[DEFINER = { user | CURRENT_USER }]*/	EVENT `test`.`dumpTag`

ON SCHEDULE
      EVERY 1 DAY
	 /* uncomment the example below you want to use */

	-- scheduleexample 1: run once

	   --  AT 'YYYY-MM-DD HH:MM.SS'/CURRENT_TIMESTAMP { + INTERVAL 1 [HOUR|MONTH|WEEK|DAY|MINUTE|...] }

	-- scheduleexample 2: run at intervals forever after creation

	   -- EVERY 1 [HOUR|MONTH|WEEK|DAY|MINUTE|...]

	-- scheduleexample 3: specified start time, end time and interval for execution
	   /*EVERY 1  [HOUR|MONTH|WEEK|DAY|MINUTE|...]

	   STARTS CURRENT_TIMESTAMP/'YYYY-MM-DD HH:MM.SS' { + INTERVAL 1[HOUR|MONTH|WEEK|DAY|MINUTE|...] }

	   ENDS CURRENT_TIMESTAMP/'YYYY-MM-DD HH:MM.SS' { + INTERVAL 1 [HOUR|MONTH|WEEK|DAY|MINUTE|...] } */

/*[ON COMPLETION [NOT] PRESERVE]
[ENABLE | DISABLE]
[COMMENT 'comment']*/

DO
	BEGIN
	    CALL tagLoader();
	END$$

DELIMITER ;

/*设置定时任务结束*/

/*启动定时任务*/
ALTER EVENT `test`.`dumpTag` ON COMPLETION PRESERVE ENABLE;