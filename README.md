# lightcouch
Library to access CouchDB built on LightSpeed and JsonRPCServer (uses httpclient)

## compile

To compile the library, you have to put **lightspeed** and **jsonrpcserver** into the same directory. Then you can perform **make runtests** on it

```
/project_root
  + -- lightspeed
  + -- jsonrpcserver
  + -- lightcouch
```

If you need to use the library in your project, put it along with other libraries into the libs folder

```
/project
 + -- libs
 |     + -- lightspeed
 |     + -- jsonrpcserver
 |     + -- lightcouch
 ```
 
