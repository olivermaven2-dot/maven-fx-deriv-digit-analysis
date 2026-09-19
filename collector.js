const WebSocket=require('ws');
const {Pool}=require('pg');

const DERIV_WS='wss://api.derivws.com/trading/v1/options/ws/public';
const DATABASE_URL=process.env.DATABASE_URL;
const RETENTION_DAYS=7;
const MAX_QUEUE=5000;

let pool=null;
let socket=null;
let reconnectTimer=null;
let reconnectDelay=2000;
let connected=false;
let marketCount=0;
let queue=[];
const lastEpochBySymbol=new Map();

function classify(name){
  const n=String(name||'').toLowerCase();
  if(n.includes('jump')) return 'Jump Indices';
  if(n.includes('volatility')) return n.includes('1s')||n.includes('1-second')?'Volatility 1s':'Volatility Indices';
  return null;
}

function lastDigit(quote,pipSize){
  const n=Number(quote);
  const p=Number(pipSize);
  if(Number.isFinite(n)&&Number.isInteger(p)&&p>=0){
    return Math.round(Math.abs(n)*Math.pow(10,p))%10;
  }
  const s=String(quote);
  const dot=s.indexOf('.');
  if(dot<0) return Number(s.slice(-1));
  const frac=s.slice(dot+1).replace(/[^0-9]/g,'');
  return Number(frac.length?frac.slice(-1):'0');
}

async function initDb(){
  if(!DATABASE_URL) throw new Error('DATABASE_URL is not configured');
  pool=new Pool({connectionString:DATABASE_URL,ssl:{rejectUnauthorized:false},max:5});
  await pool.query('CREATE TABLE IF NOT EXISTS deriv_ticks (id BIGSERIAL PRIMARY KEY,symbol TEXT NOT NULL,epoch BIGINT NOT NULL,quote DOUBLE PRECISION NOT NULL,digit SMALLINT NOT NULL,received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),UNIQUE(symbol,epoch));');
  await pool.query('CREATE INDEX IF NOT EXISTS deriv_ticks_symbol_epoch_idx ON deriv_ticks(symbol,epoch DESC);');
  await pool.query('CREATE INDEX IF NOT EXISTS deriv_ticks_received_idx ON deriv_ticks(received_at);');
}

async function flush(){
  if(!pool||queue.length===0) return;
  const batch=queue.splice(0,Math.min(queue.length,500));
  const values=[];const params=[];
  batch.forEach((t,i)=>{
    const b=i*4;
    values.push('($'+(b+1)+',$'+(b+2)+',$'+(b+3)+',$'+(b+4)+')');
    params.push(t.symbol,t.epoch,t.quote,t.digit);
  });
  try{
    await pool.query('INSERT INTO deriv_ticks(symbol,epoch,quote,digit) VALUES '+values.join(',')+' ON CONFLICT(symbol,epoch) DO NOTHING',params);
  }catch(err){
    console.error('DB flush error:',err.message);
    queue.unshift(...batch);
    if(queue.length>MAX_QUEUE) queue=queue.slice(-MAX_QUEUE);
  }
}

async function prune(){
  if(!pool) return;
  try{
    await pool.query("DELETE FROM deriv_ticks WHERE received_at < NOW() - INTERVAL '7 days'");
  }catch(err){console.error('DB retention error:',err.message)}
}

function scheduleReconnect(){
  if(reconnectTimer) return;
  reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect()},reconnectDelay);
  reconnectDelay=Math.min(reconnectDelay*2,30000);
}

function connect(){
  if(socket) try{socket.close()}catch(_){}
  socket=new WebSocket(DERIV_WS);
  socket.on('open',()=>{
    connected=true;
    reconnectDelay=2000;
    console.log('Background Deriv collector connected');
    socket.send(JSON.stringify({active_symbols:'brief',req_id:1}));
  });
  socket.on('message',raw=>{
    let m;try{m=JSON.parse(raw.toString())}catch(_){return}
    if(m.error){console.error('Deriv collector error:',m.error.message);return}
    if(m.msg_type==='active_symbols'){
      const list=(m.active_symbols||[]).filter(x=>classify(x.underlying_symbol_name||x.display_name));
      marketCount=list.length;
      console.log('Background collector markets:',marketCount);
      for(const x of list){
        const symbol=x.underlying_symbol||x.symbol;
        if(symbol) socket.send(JSON.stringify({ticks:symbol,subscribe:1}));
      }
      return;
    }
    if(m.msg_type==='tick'&&m.tick){
      const symbol=m.tick.symbol||m.tick.underlying_symbol;
      if(!symbol) return;
      const epoch=Number(m.tick.epoch||Math.floor(Date.now()/1000));
      const previous=lastEpochBySymbol.get(symbol);
      if(previous===epoch) return;
      lastEpochBySymbol.set(symbol,epoch);
      const quote=Number(m.tick.quote);
      if(!Number.isFinite(quote)) return;
      const digit=lastDigit(quote,m.tick.pip_size);
      queue.push({symbol,epoch,quote,digit});
      if(queue.length>MAX_QUEUE) queue=queue.slice(-MAX_QUEUE);
    }
  });
  socket.on('error',err=>console.error('Background collector socket error:',err.message));
  socket.on('close',()=>{
    connected=false;
    console.log('Background Deriv collector disconnected; reconnecting');
    scheduleReconnect();
  });
}

async function startCollector(){
  await initDb();
  setInterval(flush,1000);
  setInterval(prune,6*60*60*1000);
  await flush();
  await prune();
  connect();
}

function status(){
  return {running:true,connected,marketCount,queuedTicks:queue.length,retentionDays:RETENTION_DAYS};
}

module.exports={startCollector,status};