import { decodeBase64, encodeBase64 } from "jsr:@std/encoding/base64";
import { sha256 } from "https://denopkg.com/chiefbiiko/sha256@v1.0.0/mod.ts";


//import mysql from "npm:mysql2@^2.3.3/promise";
import { Client, TLSConfig, TLSMode } from "https://deno.land/x/mysql/mod.ts";
import { configLogger } from "https://deno.land/x/mysql/mod.ts";
await configLogger({ enable: false });
//const {
//  createHash,
//} = await import('node:crypto');
import { Md5 } from "https://deno.land/std@0.95.0/hash/md5.ts";

const processRequest = async (myurls: array,fixedstr: string,laststr: string): Promise<any> => {
    let fixed=JSON.parse(fixedstr)
    //console.log(JSON.stringify(first))
    let startTime=new Date().getTime()
    let connurl=new URL(Deno.env.get("DB_URL"))
    const textDecoder = new TextDecoder();
//    console.log(textDecoder.decode(decodeBase64(base64Encoded)));
    const tlsConfig: TLSConfig = {
        //mode: TLSMode.VERIFY_IDENTITY,
        //caCerts: [
        //    //await Deno.readTextFile("capath"),
        //    textDecoder.decode(decodeBase64(await Deno.env.get("DBCA_BASE")))
        //],
    };
    let dbconf={
      multipleStatements: true,
      hostname: connurl.hostname,
      username: connurl.username,
      db: connurl.pathname.replace(/^\//, ""),
      port: connurl.port,
      password: connurl.password,
      tls: tlsConfig,
      ssl: {
      //  // key: fs.readFileSync('./certs/client-key.pem'),
      //  // cert: fs.readFileSync('./certs/client-cert.pem')
      //  //ca: fs.readFileSync('./certs/ca-cert.pem'),
        ca: textDecoder.decode(decodeBase64(await Deno.env.get("DBCA_BASE"))),
      },
    }    

    //console.log("start.processing "+JSON.stringify(myurls))
    let urls_invalid=[]
    let urls_valid=[]
    let returnobj={}
    let createsql={}
    let sqlresult={}
    let sql=""
    const md5 = new Md5()
    
    console.log("connecting as "+dbconf.username+" to "+dbconf.hostname+" for "+JSON.stringify(myurls))
    //console.log(JSON.stringify(dbconf))
    //console.log(Deno.env.get("DB_URL"))
    const conn = await new Client().connect(dbconf);
    let shasend=[]
    let urlsend=[]
    for ( const idx in myurls ) {
        let presql=""
        let firstTime=startTime
        let lastTime=startTime
        let tmpsha=null
        try {
            if(Object.hasOwn(fixed,myurls[idx])) {
                firstTime=parseInt(fixed[myurls[idx]])*1000
                lastTime=parseInt(fixed[myurls[idx]])*1000
            }
        } catch(err) {
            console.log("unreadable ts first for "+ myurls[idx])
            console.log(err)
        }

        if(myurls[idx].includes("://")) {
            tmpsha=sha256(myurls[idx], "utf8", "hex")           
            //createsql[tmpsha]=await conn.query("INSERT IGNORE INTO urlhash (sha,md5,url) \nVALUES ('"+tmpsha+"','"+md5.update(myurls[idx]).toString()+"','"+myurls[idx]+"'); \n")
            shasend.push([tmpsha,md5.update(myurls[idx]).toString(),myurls[idx]])
            //presql="INSERT IGNORE INTO urlhash (sha,md5,url) VALUES ('"+tmpsha+"','"+md5.update(myurls[idx]).toString()+"','"+myurls[idx]+"') ; \n"
        } else {
            //got a hash
            if(myurls[idx].length==64 ) {
                urls_valid.push(myurls[idx])
                tmpsha=myurls[idx]
            } else {
                urls_invalid.push(myurls[idx])
            }
        }
        //sql=presql
        //sql=sql+"INSERT INTO "
        //sql=sql+" urlseen (sha,firstseen,lastseen) "
        //sql=sql+"VALUES ('"+tmpsha+"',"+parseInt(firstTime/1000)+","+parseInt(lastTime/1000)+") "
        //sql=sql+"ON DUPLICATE KEY "
        //sql=sql+"    UPDATE lastseen = GREATEST( VALUES(lastseen),lastseen , "+parseInt(lastTime/1000)+") , firstseen = LEAST(VALUES(firstseen),firstseen, "+parseInt(firstTime/1000)+"); \n"
        urlsend.push([tmpsha,parseInt(lastTime/1000),parseInt(firstTime/1000)])
        //console.log(sql)
        ////let myres=await conn.query(sql)
        ////sqlresult[tmpsha]=myres.affectedRows;
    }
    if(urlsend.length>0) {
        const counter = await conn.transaction(async (sqlcli) => {
        for (const elem of urlsend) {
        let fsql=""
        fsql=fsql+"INSERT INTO "
        fsql=fsql+" urlseen (sha,firstseen,lastseen) "
        fsql=fsql+"VALUES ('"+elem[0]+"',"+parseInt(elem[1])+","+parseInt(elem[2])+") "
        fsql=fsql+"ON DUPLICATE KEY "
        fsql=fsql+"    UPDATE lastseen = GREATEST( VALUES(lastseen),lastseen , "+parseInt(elem[2])+") , firstseen = LEAST(VALUES(firstseen),firstseen, "+parseInt(elem[1])+"); \n"
            await sqlcli.execute('fsql');
        }
          return await sqlcli.query('SELECT COUNT(*) FROM urlseen;');
        });
    }
    if(shasend.length>0) {
        const counter = await conn.transaction(async (sqlcli) => {
        for (const elem of shasend) {
        let fsql="INSERT IGNORE INTO urlhash (sha,md5,url) \nVALUES ('"+elem[0]+"','"+elem[1]+"','"+elem[2]+"'); \n"
            await sqlcli.execute('fsql');
        }
          return await sqlcli.query('SELECT COUNT(*) FROM urlseen;');
        });
    }
    
    //const results = await conn.query("SHOW DATABASES");
    //const results = await conn.query("SHOW TABLES");
    //console.log(createsql)
    //console.log(sql)
    await conn.close();
    //console.log(createsql);
    //console.log(sqlresult);
    returnobj.res={ "status": sqlresult  }

    return new Response(JSON.stringify(returnobj))
}

Deno.serve( async (req: Request) =>  { 
    //console.log(Deno.env.get("API_KEY"))
    if (req.method === "POST") {
        let mytoken= Deno.env.get("API_KEY")
        let returnobj={}
        if(!mytoken) {
            returnobj.status="ERR"
            returnobj.msg="NO_API_KEY"
            returnobj.msg_detail="set API_KEY environment variable to proceed"
            return new Response(JSON.stringify(returnobj))
        }
        if(req.headers.get("API-KEY")!=mytoken) {
            returnobj.status="ERR"
            returnobj.msg="UNAUTHORIZED"
            returnobj.msg_detail="send the HTTP-header API-KEY matching your API_KEY environment variable to proceed"
            return new Response(JSON.stringify(returnobj))  
        }
        //console.log(await req.body)
        const inbody=await req.text()
        let json={}
        try {
            //const json = await req.body.json()
            json=JSON.parse(inbody)
        } catch(e) {
            console.log("err+not+json")
            returnobj.status="ERR"
            returnobj.msg="NO JSON SENT"
            returnobj.msg_detail="please use the urls array in a JSON-POST request to submit"
            return new Response(JSON.stringify(returnobj))  
        }
        if(Object.hasOwn(json,"urls")) {
            //return await processRequest(["http://test.lan"])
            let myfixed={}
            if(Object.hasOwn(json,"ts")) {
                myfixed=json.ts
            }
            let mystr=JSON.stringify(myfixed)
            return await processRequest(json.urls,mystr)
        } else {
            console.log("err+no+urls")
            returnobj.status="ERR"
            returnobj.msg="NO JSON URLS"
            returnobj.msg_detail="please use the urls array in a JSON-POST request to submit"
            return new Response(JSON.stringify(returnobj))  
        }
    }
    return new Response("Hello_from_timestamper:"+new Date().getTime()) 
});

// urlstamp.urlseen definition
//CREATE TABLE "urlseen" (
//  "sha" varchar(64) NOT NULL,
//  "firstseen" bigint unsigned NOT NULL,
//  "lastseen" bigint unsigned NOT NULL,
//  PRIMARY KEY ("sha")
//);
//-- urlstamp.urlhash definition
//CREATE TABLE "urlhash" (
//  "sha" varchar(64) NOT NULL,
//  "md5" varchar(32) NOT NULL,
//  "url" varchar(2048) NOT NULL,
//  PRIMARY KEY ("sha")
//);
// CURL sample current time:
// curl -H "API-KEY: yourAPIkey" https://your-deno-123.deno.dev/ -H "Content-Type: application/json" -X POST  --data '{"urls":["http://test.lan"] }'  
// CURL sample with seen set ( seconds )
// curl -H "API-KEY: yourAPIkey" https://your-deno-123.deno.dev/ -H "Content-Type: application/json" -X POST  --data '{"urls":["http://test.lan"],"ts": { "http://test.lan": 123123123 }}'  
