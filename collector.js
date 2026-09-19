const WebSocket=require('ws');
const {Pool}=require('pg');
const DERIV_WS='wss://api.derivws.com/trading/v1/options/ws/public';
const DATABASE_URL=process.env.DATABASE_URL;
const RETENTION_DAYS=7,MAX_QUEUE=5000;
let pool=null,socket=null,reconnectTimer=null,reconnectDelay=2000,connected=false,marketCount=0,queue=[],lastTickAt=null,lastDbWriteAt=null,lastError=null,totalReceived=0,totalStored=0;
const lastEpochBySymbol=new Map();
const analysisBuffers=new Map();
const analysisCache=new Map();
const pairMap={over1:'under8',under8:'over1',over2:'under7',under7:'over2',over3:'under6',under6:'over3',over4:'under5',under5:'over4',over5:'under4',under4:'over5',over6:'under3',under3:'over6',over7:'under2',under2:'over7',over8:'under1',under1:'over8'};
function side(c){return c.startsWith('over')?'OVER':'UNDER'}
function threshold(c){return Number(c.replace('over','').replace('under',''))}
function qualifies(d,c){const n=threshold(c);return side(c)==='OVER'?d>n:d<n}
function calcContract(b,c){
  if(b.length<60)return {samples:b.length,ready:false};
  const cmp=pairMap[c], recent=b.slice(-60), mid=Math.max(1,Math.floor(recent.length/2));
  const prob=x=>x.length?x.filter(d=>qualifies(d,c)).length/x.length:0;
  const p=prob(b), pOld=prob(recent.slice(0,mid)), pNew=prob(recent.slice(mid));
  const avg=recent.reduce((a,d)=>a+d,0)/recent.length;
  const trend=side(c)==='OVER'?(avg-4.5)/4.5:(4.5-avg)/4.5;
  let last=recent[recent.length-1], follows=[];
  for(let i=0;i<b.length-1;i++)if(b[i]===last)follows.push(qualifies(b[i+1],c));
  const transition=follows.length?follows.filter(Boolean).length/follows.length:p;
  let bestDigit=-1,bestScore=-1,bestN=0,bestRaw=.5;
  for(let d=0;d<10;d++){
    let n=0,good=0;
    for(let i=0;i<b.length-1;i++)if(b[i]===d){n++;if(qualifies(b[i+1],c))good++}
    if(n>=8){
      const raw=good/n,shrunk=(good+2)/(n+4),weight=Math.min(1,n/30),score=.75*shrunk+.25*raw,weighted=.5+.5*(score-.5)*weight;
      if(weighted>bestScore){bestScore=weighted;bestDigit=d;bestN=n;bestRaw=raw}
    }
  }
  const entryProb=bestDigit<0?.5:bestRaw;
  const selected=p, comparison=prob(b.map((d)=>d)); // overwritten below for clarity
  const cb=calcBasic(b,cmp);
  const edge=selected-cb.p;
  const probabilityGate=selected>=.54&&edge>=.06;
  const comparisonGate=cb.p<=selected+.02;
  const directionalMomentum=side(c)==='OVER'?pNew-pOld>.03:pNew-pOld<-.03;
  const directionalTrend=side(c)==='OVER'?trend>.05:trend<-.05;
  const directionalTransition=transition>=.56;
  const entryGate=bestDigit>=0&&bestN>=8&&entryProb>=.60;
  const passed=[probabilityGate,comparisonGate,directionalMomentum,directionalTrend,directionalTransition,entryGate].filter(Boolean).length;
  let signal='WAIT';
  if(cb.p>selected+.04)signal='BLOCKED';
  else if(passed>=5)signal=side(c)+' '+threshold(c);
  else if(passed>=4)signal='WATCH '+side(c)+' '+threshold(c);
  return {ready:true,samples:b.length,selectedProbability:selected,comparisonProbability:cb.p,edge,momentum:pNew-pOld,trend,transition,entryDigit:bestDigit,entryProbability:entryProb,gatesPassed:passed,signal,updatedAt:new Date().toISOString()};
}
function calcBasic(b,c){
  if(!b.length)return {p:0};
  return {p:b.filter(d=>qualifies(d,c)).length/b.length};
}
function refreshAnalysis(symbol){
  const b=analysisBuffers.get(symbol)||[];
  const contracts={};
  for(const c of Object.keys(pairMap))contracts[c]=calcContract(b,c);
  analysisCache.set(symbol,{symbol,samples:b.length,contracts,updatedAt:new Date().toISOString()});
}
function addAnalysisTick(symbol,digit){
  let b=analysisBuffers.get(symbol);
  if(!b){b=[];analysisBuffers.set(symbol,b)}
  b.push(digit);if(b.length>500)b.shift();
  refreshAnalysis(symbol);
}


function classify(name){const n=String(name||'').toLowerCase();if(n.includes('jump'))return'Jump Indices';if(n.includes('volatility'))return n.includes('1s')||n.includes('1-second')?'Volatility 1s':'Volatility Indices';return null}
function lastDigit(quote,pipSize){const n=Number(quote),p=Number(pipSize);if(Number.isFinite(n)&&Number.isInteger(p)&&p>=0)return Math.round(Math.abs(n)*10**p)%10;const s=String(quote),dot=s.indexOf('.');if(dot<0)return Number(s.slice(-1));const frac=s.slice(dot+1).replace(/[^0-9]/g,'');return Number(frac.length?frac.slice(-1):'0')}
async function initDb(){if(!DATABASE_URL)throw new Error('DATABASE_URL is not configured');pool=new Pool({connectionString:DATABASE_URL,ssl:{rejectUnauthorized:false},max:5});await pool.query('CREATE TABLE IF NOT EXISTS deriv_ticks (id BIGSERIAL PRIMARY KEY,symbol TEXT NOT NULL,epoch BIGINT NOT NULL,quote DOUBLE PRECISION NOT NULL,digit SMALLINT NOT NULL,received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),UNIQUE(symbol,epoch));');await pool.query('CREATE INDEX IF NOT EXISTS deriv_ticks_symbol_epoch_idx ON deriv_ticks(symbol,epoch DESC);');await pool.query('CREATE INDEX IF NOT EXISTS deriv_ticks_received_idx ON deriv_ticks(received_at);')}
async function flush(){if(!pool||!queue.length)return;const batch=queue.splice(0,Math.min(queue.length,500)),values=[],params=[];batch.forEach((t,i)=>{const b=i*4;values.push('($'+(b+1)+',$'+(b+2)+',$'+(b+3)+',$'+(b+4)+')');params.push(t.symbol,t.epoch,t.quote,t.digit)});try{const r=await pool.query('INSERT INTO deriv_ticks(symbol,epoch,quote,digit) VALUES '+values.join(',')+' ON CONFLICT(symbol,epoch) DO NOTHING',params);totalStored+=r.rowCount;lastDbWriteAt=new Date().toISOString()}catch(err){lastError=err.message;console.error('DB flush error:',err.message);queue.unshift(...batch);if(queue.length>MAX_QUEUE)queue=queue.slice(-MAX_QUEUE)}}
async function prune(){if(!pool)return;try{await pool.query("DELETE FROM deriv_ticks WHERE received_at < NOW() - INTERVAL '7 days'")}catch(err){lastError=err.message;console.error('DB retention error:',err.message)}}
function scheduleReconnect(){if(reconnectTimer)return;reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect()},reconnectDelay);reconnectDelay=Math.min(reconnectDelay*2,30000)}
function connect(){if(socket)try{socket.close()}catch(_){};socket=new WebSocket(DERIV_WS);socket.on('open',()=>{connected=true;reconnectDelay=2000;lastError=null;console.log('Background Deriv collector connected');socket.send(JSON.stringify({active_symbols:'brief',req_id:1}))});socket.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch(_){return}if(m.error){lastError=m.error.message;console.error('Deriv collector error:',m.error.message);return}if(m.msg_type==='active_symbols'){const list=(m.active_symbols||[]).filter(x=>classify(x.underlying_symbol_name||x.display_name));marketCount=list.length;console.log('Background collector markets:',marketCount);for(const x of list){const symbol=x.underlying_symbol||x.symbol;if(symbol)socket.send(JSON.stringify({ticks:symbol,subscribe:1}))}return}if(m.msg_type==='tick'&&m.tick){const symbol=m.tick.symbol||m.tick.underlying_symbol;if(!symbol)return;const epoch=Number(m.tick.epoch||Math.floor(Date.now()/1000));if(lastEpochBySymbol.get(symbol)===epoch)return;lastEpochBySymbol.set(symbol,epoch);const quote=Number(m.tick.quote);if(!Number.isFinite(quote))return;const digit=lastDigit(quote,m.tick.pip_size);queue.push({symbol,epoch,quote,digit});addAnalysisTick(symbol,digit);if(queue.length>MAX_QUEUE)queue=queue.slice(-MAX_QUEUE);totalReceived++;lastTickAt=new Date().toISOString()}});socket.on('error',err=>{lastError=err.message;console.error('Background collector socket error:',err.message)});socket.on('close',()=>{connected=false;console.log('Background collector disconnected; reconnecting');scheduleReconnect()})}
async function warmAnalysis(){if(!pool)return;const r=await pool.query('SELECT symbol,epoch,digit FROM (SELECT symbol,epoch,digit,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY epoch DESC) rn FROM deriv_ticks) q WHERE rn<=500 ORDER BY symbol,epoch');for(const row of r.rows)addAnalysisTick(row.symbol,Number(row.digit))}
async function startCollector(){await initDb();await warmAnalysis();setInterval(flush,1000);setInterval(prune,6*60*60*1000);await flush();await prune();connect()}
function status(){return{running:true,connected,marketCount,queuedTicks:queue.length,totalReceived,totalStored,lastTickAt,lastDbWriteAt,lastError,retentionDays:RETENTION_DAYS,analyzedMarkets:analysisCache.size}}
async function databaseStatus(){if(!pool)return{ok:false,error:'database not initialized'};try{const r=await pool.query('SELECT COUNT(*)::bigint AS ticks, COUNT(DISTINCT symbol)::bigint AS markets, MAX(received_at) AS latest_received, MIN(received_at) AS oldest_received FROM deriv_ticks');return{ok:true,...r.rows[0]}}catch(err){return{ok:false,error:err.message}}}
function analysisStatus(symbol){return analysisCache.get(symbol)||{symbol,samples:0,contracts:{},updatedAt:null}}
async function databaseHistory(symbol,count=500){if(!pool)return{ok:false,error:'database not initialized',ticks:[]};try{const r=await pool.query('SELECT epoch,quote,digit FROM deriv_ticks WHERE symbol=$1 ORDER BY epoch DESC LIMIT $2',[symbol,count]);return{ok:true,symbol,ticks:r.rows.reverse()}}catch(err){return{ok:false,error:err.message,ticks:[]}}}
module.exports={startCollector,status,databaseStatus,databaseHistory,analysisStatus};