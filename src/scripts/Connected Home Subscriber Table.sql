%sql
DROP TABLE IF EXISTS default.ds_modeller_subscriber_forecast;
CREATE TABLE default.ds_modeller_subscriber_forecast(
forecast_date date,
forecast_subscriber double);

INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-01-01', '8388');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-02-01', '10981');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-03-01', '14379');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-04-01', '12883');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-05-01', '14480');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-06-01', '14015');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-07-01', '17699');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-08-01', '19087');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-09-01', '18345');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-10-01', '15569');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-11-01', '18253');
INSERT INTO default.ds_modeller_subscriber_forecast VALUES('2022-12-01', '16653');

select * from default.ds_modeller_subscriber_forecast order by forecast_date desc;