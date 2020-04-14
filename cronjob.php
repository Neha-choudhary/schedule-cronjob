<?php
date_default_timezone_set("Asia/Taipei");
require_once '/var/www/html/vendor/autoload.php';//這個很重要，可以直接拿到你vendor的資料
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\ExponentialBackoff;
use Google\Cloud\Core\ServiceBuilder;
putenv('GOOGLE_APPLICATION_CREDENTIALS=/var/www/html/mesmerizing-bee-273409-0b1b453c26f1.json');
class CronJamesReport{
   public function __construct(){
        $this->hi='fuck';
        //check every request start
        
        $this->requestDataSet = 'james_web_daily_rawdata';
        $this->requestTablePrefix = 'daily_request_log_';
        $this->yesterDate = date("Ymd", strtotime("-1 day"));
        $this->RequestYesterDate = date("Y-m-d", strtotime("-1 day"));
        $this->requestClient= new BigQueryClient([
            'projectId'=>'mesmerizing-bee-273409'
        ]);
        $this->requestTable = $this->requestClient
            ->dataset($this->requestDataSet)
            ->table($this->requestTablePrefix . $this->yesterDate);//daily_log_20200302
        //check every request end
        //daily_bigquery setting start
        
        $this->bigQueryDataSet = 'james_profile_daily_log';
        $this->bigQueryTablePrefix = 'daily_log_';
        // $this->yesterDate = date("Ymd", strtotime("-1 day"));
        $this->bigqueryClient= new BigQueryClient([
            'projectId'=>'mesmerizing-bee-273409'
        ]);
        $this->bigQueryTable = $this->bigqueryClient
            ->dataset($this->bigQueryDataSet)
            ->table($this->bigQueryTablePrefix . $this->yesterDate);//daily_log_20200302
        //daily_bigquery setting end


    }
    
    public function writeLog($writeContent){
        //設定目錄時間
            $years=date('Y');
            $month=date('m');
            $myday=date('Ymd');
            //設定路徑目錄資訊
            $url="/var/www/html/logs/$years/$month/$myday"."_logs.txt";
            //取出目錄路徑中目錄(不包括後面的檔案)
            $dir_name=dirname($url);//dirname是取出你給予參數的parent目錄，Ex: 你的檔案是2020-02_request_log.txt，那它存在哪裏? 就可以用dirname去找
            // dd($dir_name);// =>./public/log/texlog/2020-02，意思是$url在顯示出來的目錄底下
            // dd(is_dir($dir_name));
            //如果目錄不存在就建立
            if(!is_dir($dir_name)){//這裡用file_exists or is_dir都可以
            //iconv防止中文亂碼
            $res=mkdir(iconv("UTF-8","GBK",$dir_name),0777,true);
            }
            $fp=fopen($url,"a");//開啟檔案資源通道 不存在則自動建立
            $data=$writeContent;
            fwrite($fp,var_export($data,true)."\r\n");//寫入檔案 
            fclose($fp);//關閉資源通道
        }

    public function happy(){
        print_r('hi'.$this->hi);
    }
    public function hey(){//check $this->function
        $this->happy();
    }

    public function runDailyRequestJob(){
        //first request log settle down
        try{
        $requestIsComplete=false;
        $requestQuery=sprintf('
        SELECT * FROM `mesmerizing-bee-273409.james_web_rawdata.requests` WHERE DATE(timestamp) = "%s";
        ',$this->RequestYesterDate);
        $requestJobConfiguration = $this->requestClient
                ->query($requestQuery)
                ->allowLargeResults(true)
                ->writeDisposition('WRITE_TRUNCATE')
                ->createDisposition('CREATE_IF_NEEDED')
                ->destinationTable($this->requestTable);
            $requestResult=$this->requestClient->startQuery($requestJobConfiguration);
            $requestIsComplete = $requestResult->isComplete();
                while (!$requestIsComplete) {
                    sleep(1); // let's wait for a moment...
                    $requestResult->reload();
                    $requestIsComplete = $requestResult->isComplete();
                }
                return "success";
                
            }catch(Exception $e){
                $writeMessageTime=date("Ymd H:i:s");
                $errorMessage='runDailyRequestJob is fail';
                $this->writeLog($writeMessageTime." ".$errorMessage);
            }
    }
    



    public function runDailyLog(){
        //first request log settle down
        try{
        $bigqueryIsComplete=false;
        $bigQueryQuery=sprintf("
        CREATE TEMPORARY FUNCTION urldecode(x string)
RETURNS string
LANGUAGE js AS ''' try{return decodeURIComponent(x)}catch(e){return x}; ''';
with a as (
SELECT 
        timestamp,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+sid=([\d\w-]{0,32})')) AS site_id,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+cftuid=([\d\w-]{0,32})')) AS cft_uid,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+cf_p=([\d]{0,6}-[\d\w]{32})')) AS cft_p,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+t=(\d{8}T\d{6})')) AS log_time,
        receiveTimestamp,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+en=([^&]*)')) AS event_name,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+ea=([^&]*)')) AS event_action,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+el=([^&]*)')) AS event_labe,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+ec=([^&]*)')) AS event_category,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+ev=([\d-]{0,10})')) AS event_value,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+de=([\d\w-]{0,16})')) AS document_encode,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+ul=([\d\w-]{0,16})')) AS document_language,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+if=([\w]{1})')) AS in_iframe,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+tt=([^&]*)')) AS document_title,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+rf=([^&]*)')) AS document_referrer,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+uh=([^&]*)')) AS url_host,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+up=([^&]*)')) AS url_path,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_id=([^&]*)')) AS utm_id,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_campaign=([^&]*)')) AS utm_campaign,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_source=([^&]*)')) AS utm_source,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_medium=([^&]*)')) AS utm_medium,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_term=([^&]*)')) AS utm_term,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+utm_content=([^&]*)')) AS utm_content,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+gclid=([^&]*)')) AS gclid,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+dclid=([^&]*)')) AS dclid,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+gclsrc=([^&]*)')) AS gclsrc,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+sc=(\d*x\d*x\d*)')) AS screen,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+bn=([\w]+)')) AS browser_name,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+bv=([\d\.]+)') )AS browser_version,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+pn=([\w]+)')) AS platform_name,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+pv=([\d\.]+)')) AS platform_version,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+dv=([\w]+)')) AS deview_vendor,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+dm=([^&]*)')) AS device_model,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+dt=([\w]+)'))AS device_type,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+tz=([-]?[\d]{0,2})')) AS time_zone,
        urldecode(REGEXP_EXTRACT(httpRequest.requestUrl, r'^.+tu=(\d{1,2})')) AS touch_event,
        httpRequest.remoteIp as remote_IP,
        NET.IPV4_TO_INT64(NET.IP_FROM_STRING(httpRequest.remoteIp)) AS ip,
        TRUNC(NET.IPV4_TO_INT64(NET.IP_FROM_STRING(httpRequest.remoteIp)) / (256 * 256 * 256)) AS ip_class_a,
        httpRequest.userAgent as userAgent 
        FROM 
             `mesmerizing-bee-273409.james_web_rawdata.requests`)
   select * from a where (site_id is not null and site_id <> 'null' and cft_uid is not null and cft_uid<>'null') and DATE(timestamp) = '%s' ;
   
        ",$this->RequestYesterDate);
        $bigqueryJobConfiguration = $this->bigqueryClient
                ->query($bigQueryQuery)
                ->allowLargeResults(true)
                ->writeDisposition('WRITE_TRUNCATE')
                ->createDisposition('CREATE_IF_NEEDED')
                ->destinationTable($this->bigQueryTable);
            $bigQueryResult=$this->bigqueryClient->startQuery($bigqueryJobConfiguration);
            $bigqueryIsComplete = $bigQueryResult->isComplete();
                while (!$bigqueryIsComplete) {
                    sleep(1); // let's wait for a moment...
                    $bigQueryResult->reload();
                    $bigqueryIsComplete = $bigQueryResult->isComplete();
                }
                return "success";
                
            }catch(Exception $e){
                $writeMessageTime=date("Ymd H:i:s");
                $errorMessage='runDailyLog is fail';
                $this->writeLog($writeMessageTime." ".$errorMessage);
            }
    }
    
   
}
$bigqueryRunJob=new CronJamesReport;//實體化
// $bigqueryRunJob->hey();//check the function did it work?
// $bigqueryRunJob->writeLog("fuckyou");//test code


    $bigqueryrequestSuccessorNot=$bigqueryRunJob->runDailyRequestJob();
    if($bigqueryrequestSuccessorNot=="success"){
        $writeMessageTime=date("Ymd H:i:s");
        $message='runDailyRequestJob is success';
        $bigqueryRunJob->writeLog($writeMessageTime." ".$message);
        print_r("finish");
    }else{
        $writeMessageTime=date("Ymd H:i:s");
        $errorMessage='runDailyRequestJob is fail';
        $bigqueryRunJob->writeLog($writeMessageTime." ".$errorMessage);
        print_r("runDailyRequestJob is fail");
    }
    
    $runDailyLogSuccessorNot=$bigqueryRunJob->runDailyLog();
    if($runDailyLogSuccessorNot=="success"){
        $writeMessageTime=date("Ymd H:i:s");
        $message='runDailyLog is success';
        $bigqueryRunJob->writeLog($writeMessageTime." ".$message);
        print_r("finish");
    }else{
        $writeMessageTime=date("Ymd H:i:s");
        $errorMessage='runDailyLog is fail';
        $bigqueryRunJob->writeLog($writeMessageTime." ".$errorMessage);
        print_r("runDailyLog is fail");
    }
?>
