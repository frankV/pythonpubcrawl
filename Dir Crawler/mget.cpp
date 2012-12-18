#include <iostream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stringo.h>
#include <utils.h>
#include <linkedlist.h>
#include <web/web.h>
#include <web/login.h>
#include <sys/stat.h>
#include <ftw.h>
#include <time.h>
#include <MySQL.h>
#include <pthread.h>
#include <datetime.h>

login::Directives directives;
login::Args args;
struct LocalArgs {
  String directory,suggname,mget_disposition;
  long long clen;
  LinkedList<String> filelist;
  bool allFiles;
} local_args;
struct ThreadStruct {
  String command;
  pthread_t tid;
};
struct SEntry {
  String key;
};
String tdir;
String server_root="/"+getHostName().getToken(".",0);
const String doc_root=getenv("DOCUMENT_ROOT");
const String configFile(doc_root+"/cgi-bin/internal/conf/login.conf");

void parseQueryString()
{
  size_t contentLength;
  char *buffer;
  String queryString,sdum;
  StringParts sp,sp2;
  size_t n;
  struct stat buf;

  local_args.allFiles=false;
  if (getenv("CONTENT_LENGTH") != NULL) {
    contentLength=atoi(getenv("CONTENT_LENGTH"));
    buffer=new char[contentLength];
    fread(buffer,1,contentLength,stdin);
    queryString.fill(buffer,contentLength);
  }
  if (queryString.getLength() > 0) {
    convertCGICodes(queryString);
    sp.fill(queryString,"&");
    if (sp.getLength() > 0) {
	for (n=0; n < sp.getLength(); n++) {
	  if (sp.getPart(n).beginsWith("file=")) {
	    sdum=sp.getPart(n);
	    sdum.replace("file=","");
	    if (sdum == "all")
		local_args.allFiles=true;
	    else
		local_args.filelist.attach(sdum);
	  }
	  else if (sp.getPart(n).contains("sfile")) {
	    sp2.fill(sp.getPart(n),"=");
	    local_args.filelist.attach(sp2.getPart(1));
	  }
	  else if (sp.getPart(n).endsWith("=on")) {
	    sdum=sp.getPart(n);
	    sdum.replace("=on","");
	    local_args.filelist.attach(sdum);
	  }
	  else if (sp.getPart(n).beginsWith("suggname=")) {
	    sdum=sp.getPart(n);
	    sdum.replace("suggname=","");
	    local_args.suggname=sdum;
	  }
	  else if (sp.getPart(n).beginsWith("directory=")) {
	    sdum=sp.getPart(n);
	    sdum.replace("directory=","");
	    local_args.directory=sdum;
	    if (!local_args.directory.endsWith("/"))
		local_args.directory+="/";
	  }
	}
    }
  }
  else
    webError(String("no input"));
  if (!local_args.allFiles && local_args.filelist.getLength() == 0)
    webError(String("bad input - ")+queryString);
  if (local_args.directory.getLength() == 0) {
    local_args.directory.fill(getenv("HTTP_REFERER"));
    local_args.directory=local_args.directory.substr(local_args.directory.isAt("dss.ucar.edu")+12);
    if (!local_args.directory.endsWith("/"))
	local_args.directory=local_args.directory.substr((size_t)0,local_args.directory.indexOf("/",-local_args.directory.getLengthAsInt()));
  }
  if (stat(local_args.directory.toChar(),&buf) != 0) {
    if (stat(("/glade/data02"+local_args.directory).toChar(),&buf) == 0)
	local_args.directory="/glade/data02"+local_args.directory;
    else if (stat(("/glade/data02/dsstransfer"+local_args.directory).toChar(),&buf) == 0)
	local_args.directory="/glade/data02/dsstransfer"+local_args.directory;
    else if (stat((doc_root+local_args.directory).toChar(),&buf) == 0)
	local_args.directory=doc_root+local_args.directory;
    else
	webError("unable to locate data files");
  }
  if (local_args.suggname.getLength() == 0)
    local_args.suggname="datafiles.tar";
}

extern "C" int findFiles(const char *name,const struct stat64 *data,int flag,struct FTW *ftw_struct)
{
  if (flag == FTW_F)
    local_args.clen+=(long long)((data->st_size+511)/512)*512+512;
  return 0;
}

void statFiles(LinkedList<String>& statFilelist)
{
  struct stat buf;

  local_args.filelist.goStart();
  while (local_args.filelist.isCurrent()) {
    if (stat((local_args.directory+local_args.filelist.getCurrent()).toChar(),&buf) == 0) {
	local_args.clen+=(long long)((buf.st_size+511)/512)*512+512;
	statFilelist.attach(local_args.filelist.getCurrent());
    }
    local_args.filelist.advance();
  }
}

extern "C" void *outputTar(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  system((t->command).toChar());
  return NULL;
}

void tar()
{
  LinkedList<String> statFilelist;
  std::ofstream ofs,log;
  String sysString;
  FILE *p;
  String raddr,referer,uagent,login,sdum;
  StringParts sp;
  time_t tm;
  struct tm *p_tm;
  char dum[256];
  String sline,output,error;
  int idx;
  ThreadStruct t;
  MySQLServer server;
  MySQLLocalQuery query;
  MySQLRow row;
  HashTable<SEntry> noaccUser;
  SEntry se;
  std::ifstream ifs;
  char line[32768];

  server.connect(directives.database_server_host,directives.database_server_user,directives.database_server_password,directives.database_server_default,directives.database_server_timeout);
  if (!server.isConnected())
    webError("unable to connect to database");
  raddr=getenv("REMOTE_ADDR");
  if (raddr.getLength() == 0)
    raddr.fill("-");
  referer=getenv("HTTP_REFERER");
  if (referer.getLength() == 0)
    referer.fill("-");
  uagent=getenv("HTTP_USER_AGENT");
  if (uagent.getLength() == 0)
    uagent.fill("-");
  login.fill(getenv("REMOTE_USER"));
  if (login.getLength() == 0) {
    login=getDUser();
    ifs.open((doc_root+"/.htaccess").toChar());
    if (!ifs.is_open())
	webError("unable to confirm authorization");
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sp.fill(line);
	if (sp.getLength() == 4 && sp.getPart(0) == "SetEnvIf" && sp.getPart(3) == "noaccess") {
	  se.key=sp.getPart(2);
	  se.key.replace("\"","");
	  if (!noaccUser.foundEntry(se.key,se))
	    noaccUser.add(se);
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.open("/glade/data02/dsszone/.htaccess");
    if (!ifs.is_open())
	webError("unable to confirm authorization");
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sp.fill(line);
	if (sp.getLength() == 4 && sp.getPart(0) == "SetEnvIf" && sp.getPart(3) == "noaccess") {
	  se.key=sp.getPart(2);
	  se.key.replace("\"","");
	  if (!noaccUser.foundEntry(se.key,se))
	    noaccUser.add(se);
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    if (noaccUser.foundEntry(login,se)) {
	std::cout << "Content-Type: text/plain" << std::endl << std::endl;
	std::cout << "Forbidden" << std::endl;
	exit(1);
    }
    if (login.getLength() == 0 && local_args.directory.beginsWith("/glade/data02/dsstransfer/download/")) {
	login=local_args.directory.substitute("/glade/data02/dsstransfer/download/","");
	if ( (idx=login.indexOf("/")) > 0)
	  login=login.substr((size_t)0,idx);
    }
    if (login.getLength() == 0) {
	if (local_args.directory.beginsWith("/glade/data02")) {
	  std::cout << "Content-Type: text/plain" << std::endl << std::endl;
	  std::cout << "Error: cookies must be enabled in your browser and you must be signed in." << std::endl;
	  std::cout << "If you continue to have trouble, please contact dssweb@ucar.edu." << std::endl;
	  exit(1);
	}
	login.fill("-");
    }
    else {
	query.set("process","mget_process","login = '"+login+"'");
	if (query.submit(server) < 0)
	  webError(query.getError());
	if (query.getCurrentResult(row) == 0) {
	  sdum=row.getColumn(0);
	  while (sdum.getLength() < 5)
	    sdum=" "+sdum;
	  p=popen(("ps -u apache |grep \""+sdum+" ?\"").toChar(),"r");
	  if (fgets(dum,256,p) != NULL) {
	    std::cout << "Content-Type: text/plain" << std::endl << std::endl;
	    std::cout << "You are currently downloading a file." << std::endl;
	    std::cout << "You cannot initiate another download until the current one finishes." << std::endl;
	    std::cout << "Use your browser's \"Back\" button to go back." << std::endl;
	    exit(1);
	  }
	  else
	    server._delete("mget_process","login = '"+login+"'");
	  pclose(p);
	}
    }
  }
  tm=time(NULL);
  p_tm=localtime(&tm);
  strftime(dum,sizeof(dum),(char *)"[%a %b %e %H:%M:%S %Z %Y]",p_tm);
  local_args.clen=0;
  log.open((server_root+"/logs/mget_log").toChar(),std::fstream::app);
  if (!log.is_open())
	webError("unable to open log file");
  if (local_args.allFiles && local_args.filelist.getLength() == 0) {
    nftw64(local_args.directory.toChar(),findFiles,20,0);
    if (local_args.clen > 0x7fffffff)
	webError("size of tar file exceeds 2 Gigabytes - unable to proceed");
    local_args.clen+=1536;
    log << raddr << " - " << login << " " << dum << " - 200 " << local_args.clen << " \"" << referer << "\" \"" << uagent << "\" *" << std::endl;
    log.close();
    std::cout << "Content-disposition: inline; filename=" << local_args.suggname << std::endl;
    std::cout << "Content-Type: application/x-tar" << std::endl;
    std::cout << "Content-Length: " << local_args.clen << std::endl;
    std::cout << std::endl;
    sysString=String("cd ")+local_args.directory+"; /bin/tar chf - *";
    system(sysString.toChar());
  }
  else if (local_args.filelist.getLength() > 0) {
    statFiles(statFilelist);
    if (statFilelist.getLength() == 0) {
	webError("no files found");
	exit(1);
    }
    else if (statFilelist.getLength() == 1) {
	webError("To use this feature, please select more than one file.  Otherwise, use the your browser's \"Back\" button to go back and download the single file you want by clicking it's link.");
	exit(1);
    }
    if (local_args.clen > 0x7fffffff)
	webError("size of tar file exceeds 2 Gigabytes - unable to proceed");
    local_args.clen+=1536;
    log << raddr << " - " << login << " " << dum << " - 200 " << local_args.clen << " \"" << referer << "\" \"" << uagent << "\" " << statFilelist.getLength()+1 << std::endl;
    log << local_args.directory << std::endl;
    tdir=strand(15)+".mget";
    if (mysystem("/bin/mkdir "+server_root+"/tmp/"+tdir,output,error) < 0)
	webError(error);
    if (mysystem("/bin/chmod 777 "+server_root+"/tmp/"+tdir,output,error) < 0)
	webError(error);
    statFilelist.goStart();
    while (statFilelist.isCurrent()) {
	log << statFilelist.getCurrent() << std::endl;
	idx=statFilelist.getCurrent().indexOf("/",-statFilelist.getCurrent().getLengthAsInt());
	system(("ln -s "+local_args.directory+statFilelist.getCurrent()+" "+server_root+"/tmp/"+tdir+"/"+statFilelist.getCurrent().substr(idx+1)).toChar());
	ofs << statFilelist.getCurrent() << std::endl;
	statFilelist.advance();
    }
    log.close();
    t.command="/bin/tar chf - -C "+server_root+"/tmp/"+tdir+" .";
    std::cout << "Content-disposition: inline; filename=" << local_args.suggname << std::endl;
    std::cout << "Content-Type: application/x-tar" << std::endl;
    std::cout << "Content-Length: " << local_args.clen << std::endl;
    std::cout << std::endl;
    pthread_create(&t.tid,NULL,outputTar,(void *)&t);
    p=popen(("ps -u apache -o pid,comm,args |grep "+tdir).toChar(),"r");
    while (fgets(dum,256,p) != NULL) {
	sline.fill(dum);
	sline.trim();
	sp.fill(sline);
	if (sp.getLength() > 1 && sp.getPart(2).beginsWith("/bin/tar")) {
	  if (server.insert("mget_process","'"+login+"',"+sp.getPart(0)+",'"+getCurrentDateTime().toString("%Y-%m-%d %H:%MM:%SS")+"'") < 0)
	    std::cerr << server.getError() << " while inserting ' '" << login << "'," << sp.getPart(0) << "'" << std::endl;
	}
    }
    pclose(p);
    server.disconnect();
    pthread_join(t.tid,NULL);
    if (tdir.getLength() == 20)
	system(("rm -rf "+server_root+"/tmp/"+tdir).toChar());
  }
}

int main(int argc,char **argv)
{
  std::ifstream ifs;
  char line[32768];
  StringParts sp;
  String host,serverAddr;

  host.fill(getenv("HTTP_HOST"));
  serverAddr.fill(getenv("SERVER_ADDR"));
  if ((host != "rda.ucar.edu" && host != "dss.ucar.edu" && host != "castle.ucar.edu" && host != "bross.ucar.edu") || (serverAddr != "128.117.222.210" && serverAddr != "128.117.222.75" && serverAddr != "128.117.222.76")) {
    webError(String("unauthorized server"));
    exit(1);
  }
  ifs.open(configFile.toChar());
  if (!ifs)
    webError(String("unable to open config file"));
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (line[0] != '#') {
	sp.fill(line);
	if (sp.getPart(0) == "DatabaseServerHost") {
	  if (sp.getLength() > 1)
	    directives.database_server_host=sp.getPart(1);
	}
	else if (sp.getPart(0) == "DatabaseServerUser")
	  directives.database_server_user=sp.getPart(1);
	else if (sp.getPart(0) == "DatabaseServerPassword")
	  directives.database_server_password=sp.getPart(1);
	else if (sp.getPart(0) == "DatabaseServerDefault")
	  directives.database_server_default=sp.getPart(1);
	else if (sp.getPart(0) == "DatabaseServerTimeout")
	  directives.database_server_timeout=atoi(sp.getPart(1).toChar());
	else if (sp.getPart(0) == "LogDirectory")
	  directives.log_directory=sp.getPart(1);
	else if (sp.getPart(0) == "mget.tar")
	  local_args.mget_disposition=sp.getPart(1);
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  if (local_args.mget_disposition == "disabled") {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "Temporarily unavailable - we apologize for the inconvenience" << std::endl;
    exit(1);
  }
  parseQueryString();
  tar();
}
