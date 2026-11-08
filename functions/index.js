// منصة الفرص – دالة الذكاء الاصطناعي (fpai)
// الجمعية بتبعت طلب الكتابة هنا مع توكن الدخول، والدالة بتتأكد إن الحساب ساري وإن المستخدم مدير حساب أو مُعِد،
// وبعدين تكلم Claude API بالمفتاح المحفوظ في Secret Manager وترجع الرد، وتسجل الاستهلاك لكل جمعية في fp_usage.
const {onRequest}=require('firebase-functions/v2/https');
const {defineSecret}=require('firebase-functions/params');
const admin=require('firebase-admin');
const {FieldValue}=require('firebase-admin/firestore');
admin.initializeApp();
const db=admin.firestore();
const KEY=defineSecret('ANTHROPIC_API_KEY');

const ADMIN_EMAIL='elzieni2007@gmail.com';
const ORIGINS=['https://elzieni2007-crypto.github.io'];
// المقترح الكامل والترجمة بـ Opus، والاستخراج والأفكار والتحليل بـ Sonnet، والمهام السريعة بـ Haiku
const MODELS={full:process.env.FP_MODEL_FULL||'claude-opus-5-5',std:process.env.FP_MODEL_STD||'claude-sonnet-5-5',quick:process.env.FP_MODEL_QUICK||'claude-haiku-5-5'};
const MAXTOK={full:16000,std:8000,quick:4000};
const MAX_PROMPT=400000;
const BASE=process.env.ANTHROPIC_BASE_URL||'https://api.anthropic.com';
const JSON_SYS='You are a careful assistant for Egyptian NGOs. Reply with exactly one valid JSON value and nothing else: no markdown fences, no commentary. Never invent facts, numbers or requirements that are not in the input.';
const TEXT_SYS='You are a careful proposal-writing assistant for Egyptian NGOs. Follow the user instructions exactly. Never invent facts, figures, partners or results that are not in the input; where data is missing, leave the placeholder the instructions ask for.';

const bad=(res,code,error)=>{if(!res.headersSent)res.status(code).json({error})};

async function who(req){
  const h=req.get('Authorization')||'';const m=h.match(/^Bearer (.+)$/);if(!m)throw [401,'لازم تسجل دخول الأول'];
  let t;try{t=await admin.auth().verifyIdToken(m[1])}catch(e){throw [401,'انتهت جلسة الدخول. سجل دخول تاني']}
  if(t.email===ADMIN_EMAIL)return {uid:t.uid,orgId:'_admin',name:'الإدارة'};
  const u=await db.doc('users/'+t.uid).get();const orgId=u.exists&&u.data().orgId;if(!orgId)throw [403,'الحساب ده مش مربوط بجمعية'];
  const [o,mm]=await Promise.all([db.doc('orgs/'+orgId).get(),db.doc('orgs/'+orgId+'/members/'+t.uid).get()]);
  const org=o.data()||{},mem=mm.data()||{};
  const until=org.paidUntil&&org.paidUntil.toDate?org.paidUntil.toDate():null;
  if(org.status!=='active'||!until||until<new Date())throw [403,'اشتراك الجمعية متوقف أو منتهي. تواصل مع إدارة تنمويين مصر'];
  if(mem.status!=='active')throw [403,'حسابك متوقف من مدير حساب الجمعية'];
  if(!['owner','editor'].includes(mem.role))throw [403,'صلاحيتك «مراجع» مش بتسمح بالكتابة بالذكاء الاصطناعي'];
  return {uid:t.uid,orgId,name:mem.name||t.email};
}

async function logUsage(orgId,tier,model,usage){
  const mon=new Date().toISOString().slice(0,7);const inc=FieldValue.increment;
  const i=(usage&&usage.input_tokens)||0,o=(usage&&usage.output_tokens)||0;
  await db.doc(`fp_usage/${orgId}_${mon}`).set({orgId,month:mon,calls:inc(1),inTok:inc(i),outTok:inc(o),
    by:{[tier]:{calls:inc(1),in:inc(i),out:inc(o)}},model:{[tier]:model},
    last:FieldValue.serverTimestamp()},{merge:true}).catch(e=>console.error('usage',e));
}

// بيكلم Claude بالـ streaming وبيبعت كل جزء نص لـ onDelta
async function claude({model,maxTok,system,prompt,signal,onDelta}){
  const r=await fetch(BASE+'/v1/messages',{method:'POST',signal,headers:{'content-type':'application/json','x-api-key':KEY.value(),'anthropic-version':'2023-06-01'},
    body:JSON.stringify({model,max_tokens:maxTok,stream:true,system,messages:[{role:'user',content:prompt}]})});
  if(!r.ok){const t=await r.text();console.error('anthropic',r.status,t.slice(0,500));throw new Error(r.status===429||r.status===529?'الخدمة عليها ضغط دلوقتي. جرب كمان دقيقة':'خدمة الكتابة رجعت خطأ ('+r.status+')')}
  const rd=r.body.getReader(),td=new TextDecoder();let buf='',text='',stop='',usage={input_tokens:0,output_tokens:0};
  for(;;){const {value,done}=await rd.read();if(done)break;buf+=td.decode(value,{stream:true});let i;
    while((i=buf.indexOf('\n'))>=0){const line=buf.slice(0,i).trim();buf=buf.slice(i+1);if(!line.startsWith('data:'))continue;let ev;try{ev=JSON.parse(line.slice(5))}catch(e){continue}
      if(ev.type==='message_start'&&ev.message&&ev.message.usage)usage.input_tokens=ev.message.usage.input_tokens||0;
      else if(ev.type==='content_block_delta'&&ev.delta&&ev.delta.type==='text_delta'){text+=ev.delta.text;onDelta&&onDelta(ev.delta.text)}
      else if(ev.type==='message_delta'){if(ev.delta&&ev.delta.stop_reason)stop=ev.delta.stop_reason;if(ev.usage&&ev.usage.output_tokens)usage.output_tokens=ev.usage.output_tokens}
      else if(ev.type==='error')throw new Error('خدمة الكتابة رجعت خطأ: '+((ev.error&&ev.error.message)||''))}}
  return {text,stop,usage};
}

function parseJSON(t){let s=String(t).trim().replace(/^```(?:json)?\s*/i,'').replace(/```\s*$/,'');
  try{return JSON.parse(s)}catch(e){}
  const a=Math.min(...['{','['].map(c=>{const k=s.indexOf(c);return k<0?Infinity:k}));const b=Math.max(s.lastIndexOf('}'),s.lastIndexOf(']'));
  if(a<Infinity&&b>a)return JSON.parse(s.slice(a,b+1));throw new Error('bad json')}

exports.fpai=onRequest({secrets:[KEY],region:'us-central1',cors:ORIGINS.concat(process.env.FP_EXTRA_ORIGIN?[process.env.FP_EXTRA_ORIGIN]:[]),timeoutSeconds:540,memory:'512MiB',maxInstances:20,concurrency:40},async(req,res)=>{
  if(req.method!=='POST')return bad(res,405,'POST only');
  let me;try{me=await who(req)}catch(e){return Array.isArray(e)?bad(res,e[0],e[1]):bad(res,500,'حصلت مشكلة في التحقق من الحساب')}
  const {mode,prompt}=req.body||{};const tier=['full','std','quick'].includes(req.body&&req.body.tier)?req.body.tier:'std';
  if(typeof prompt!=='string'||!prompt.trim())return bad(res,400,'الطلب فاضي');
  if(prompt.length>MAX_PROMPT)return bad(res,413,'النص طويل جدًا. قلل حجم المستندات المرفوعة');
  const model=MODELS[tier],ctl=new AbortController();res.on('close',()=>{if(!res.writableEnded)ctl.abort()});
  if(mode==='json'){
    try{const out=await claude({model,maxTok:MAXTOK[tier],system:JSON_SYS,prompt,signal:ctl.signal});await logUsage(me.orgId,tier,model,out.usage);
      let data;try{data=parseJSON(out.text)}catch(e){return bad(res,502,'الرد ما كانش بالشكل المطلوب. جرب تاني')}
      return res.json({data})}catch(e){if(ctl.signal.aborted)return;return bad(res,502,e.message)}}
  // mode text: NDJSON stream {t:"…"} … {end:true,truncated}
  res.status(200).set({'Content-Type':'application/x-ndjson; charset=utf-8','Cache-Control':'no-cache','X-Accel-Buffering':'no'});
  if(res.flushHeaders)res.flushHeaders();
  try{const out=await claude({model,maxTok:MAXTOK[tier],system:TEXT_SYS,prompt,signal:ctl.signal,onDelta:t=>res.write(JSON.stringify({t})+'\n')});
    await logUsage(me.orgId,tier,model,out.usage);res.write(JSON.stringify({end:true,truncated:out.stop==='max_tokens'})+'\n');res.end()}
  catch(e){if(ctl.signal.aborted)return;res.write(JSON.stringify({err:e.message})+'\n');res.end()}
});

// ===== fpteam: ربط الأشخاص بالجمعيات =====
// الموقع الرئيسي والمنصة على نفس نظام الدخول، فالإيميل ممكن يكون عليه حساب بالفعل (مورد، باحث عن عمل، مؤسسة…).
// الدالة دي بتربط الحساب الموجود بالجمعية بدل ما تعمل حساب جديد، وبتسمح لنفس الشخص يبقى في أكتر من جمعية.
// users/{uid}: orgs = كل الجمعيات اللي هو فيها، orgId = الجمعية المفتوحة دلوقتي.
const ROLES=['owner','editor','reviewer'];
const caller=async req=>{const h=req.get('Authorization')||'';const m=h.match(/^Bearer (.+)$/);if(!m)throw [401,'لازم تسجل دخول الأول'];
  try{return await admin.auth().verifyIdToken(m[1])}catch(e){throw [401,'انتهت جلسة الدخول. سجل دخول تاني']}};
const orgOk=o=>{const u=o&&o.paidUntil&&o.paidUntil.toDate?o.paidUntil.toDate():null;return !!o&&o.status==='active'&&!!u&&u>new Date()};
const rndPw=()=>require('crypto').randomBytes(24).toString('base64url')+'Aa1!';

exports.fpteam=onRequest({region:'us-central1',cors:ORIGINS.concat(process.env.FP_EXTRA_ORIGIN?[process.env.FP_EXTRA_ORIGIN]:[]),maxInstances:10},async(req,res)=>{
  if(req.method!=='POST')return bad(res,405,'POST only');
  let t;try{t=await caller(req)}catch(e){return bad(res,e[0],e[1])}
  const isAdm=t.email===ADMIN_EMAIL,b=req.body||{},action=b.action;
  try{
    if(action==='mine'){
      const u=await db.doc('users/'+t.uid).get();const ud=u.data()||{};const ids=[...new Set([...(ud.orgs||[]),...(ud.orgId?[ud.orgId]:[])])];
      const out=[];for(const id of ids){const [o,m]=await Promise.all([db.doc('orgs/'+id).get(),db.doc('orgs/'+id+'/members/'+t.uid).get()]);if(!o.exists||!m.exists)continue;
        const od=o.data(),md=m.data();out.push({orgId:id,name:od.name||'',role:md.role,status:md.status,orgActive:orgOk(od)})}
      return res.json({current:ud.orgId||'',orgs:out})}
    if(action==='switch'){
      const orgId=String(b.orgId||'');const m=await db.doc('orgs/'+orgId+'/members/'+t.uid).get();
      if(!m.exists||m.data().status!=='active')return bad(res,403,'إنت مش عضو نشط في الجمعية دي');
      await db.doc('users/'+t.uid).set({orgId,orgs:FieldValue.arrayUnion(orgId)},{merge:true});return res.json({ok:true})}
    if(action==='add'){
      const orgId=String(b.orgId||''),email=String(b.email||'').trim().toLowerCase(),name=String(b.name||'').trim().slice(0,120),role=String(b.role||'');
      if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))return bad(res,400,'الإيميل مكتوب غلط');
      if(!ROLES.includes(role))return bad(res,400,'الدور مش صحيح');
      const oRef=db.doc('orgs/'+orgId),o=await oRef.get();if(!orgId||!o.exists)return bad(res,404,'الجمعية مش موجودة');
      const od=o.data();
      if(!isAdm){
        const me=await db.doc('orgs/'+orgId+'/members/'+t.uid).get();const md=me.data()||{};
        if(md.role!=='owner'||md.status!=='active')return bad(res,403,'إضافة الزملاء لمدير حساب الجمعية بس');
        if(!orgOk(od))return bad(res,403,'اشتراك الجمعية متوقف أو منتهي');
        if(role==='owner')return bad(res,403,'مدير الحساب واحد بس، والإدارة هي اللي تغيره');
        const act=await oRef.collection('members').where('status','==','active').get();
        if(act.size>=(+od.seats||3))return bad(res,409,'وصلت للحد الأقصى للمستخدمين');
      }
      let user,existed=true;
      try{user=await admin.auth().getUserByEmail(email)}catch(e){if(e.code!=='auth/user-not-found')throw e;existed=false;
        user=await admin.auth().createUser({email,password:rndPw(),displayName:name||undefined})}
      const mRef=oRef.collection('members').doc(user.uid),mm=await mRef.get();
      if(mm.exists&&!(isAdm&&role==='owner'))return bad(res,409,mm.data().status==='active'?'الشخص ده موجود في الفريق بالفعل':'الشخص ده في الفريق بس موقوف. شغّله من قائمة الفريق');
      const now=FieldValue.serverTimestamp(),uRef=db.doc('users/'+user.uid),ud=(await uRef.get()).data()||{};
      let keepCur=false;if(ud.orgId&&ud.orgId!==orgId){const cm=await db.doc('orgs/'+ud.orgId+'/members/'+user.uid).get();keepCur=cm.exists&&cm.data().status==='active'}
      const bt=db.batch();
      bt.set(mRef,{email,name:name||(mm.exists?mm.data().name:'')||'',role,status:'active',addedAt:now,addedBy:t.email||''},{merge:true});
      bt.set(uRef,Object.assign({orgs:FieldValue.arrayUnion(orgId)},keepCur?{}:{orgId}),{merge:true});
      if(role==='owner')bt.update(oRef,{ownerUid:user.uid,email,updatedAt:now});
      await bt.commit();
      return res.json({ok:true,uid:user.uid,existed})}
    return bad(res,400,'طلب غير معروف')}
  catch(e){console.error('fpteam',e);return bad(res,500,'حصلت مشكلة: '+(e.message||e))}
});
