const express=require('express');
const path=require('path');
const {startCollector,status:collectorStatus,databaseStatus,databaseHistory}=require('./collector');
const app=express(),PORT=process.env.PORT||3000;
app.use(express.static(path.join(__dirname,'public')));
app.get('/health',async(req,res)=>res.json({ok:true,app:'Maven FX Deriv Digit Analysis',collector:collectorStatus(),database:await databaseStatus()}));
app.get('/collector-status',async(req,res)=>res.json({collector:collectorStatus(),database:await databaseStatus()}));
app.get('/market-history',async(req,res)=>{
  const symbol=String(req.query.symbol||'');
  const count=Math.min(500,Math.max(1,Number(req.query.count)||500));
  if(!symbol)return res.status(400).json({ok:false,error:'symbol is required'});
  res.json(await databaseHistory(symbol,count));
});
app.use((req,res)=>res.sendFile(path.join(__dirname,'public','index.html')));
app.listen(PORT,'0.0.0.0',()=>console.log('Maven FX running on '+PORT));
startCollector().catch(err=>console.error('Background collector failed to start:',err));