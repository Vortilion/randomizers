var FI=Object.defineProperty,PI=Object.defineProperties;var LI=Object.getOwnPropertyDescriptors;var sc=Object.getOwnPropertySymbols;var Av=Object.prototype.hasOwnProperty,Ov=Object.prototype.propertyIsEnumerable;var Rv=(t,n,e)=>n in t?FI(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,b=(t,n)=>{for(var e in n||={})Av.call(n,e)&&Rv(t,e,n[e]);if(sc)for(var e of sc(n))Ov.call(n,e)&&Rv(t,e,n[e]);return t},J=(t,n)=>PI(t,LI(n));var gm=(t,n)=>{var e={};for(var i in t)Av.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&sc)for(var i of sc(t))n.indexOf(i)<0&&Ov.call(t,i)&&(e[i]=t[i]);return e};var At=null,lc=!1,_m=1,BI=null,Xe=Symbol("SIGNAL");function ne(t){let n=At;return At=t,n}function cc(){return At}var ki={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ri(t){if(lc)throw new Error("");if(At===null)return;At.consumerOnSignalRead(t);let n=At.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=At.recomputing;if(i&&(e=n!==void 0?n.nextProducer:At.producers,e!==void 0&&e.producer===t)){At.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===At&&(!i||VI(r,At)))return;let o=Do(At),a={producer:t,consumer:At,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};At.producersTail=a,n!==void 0?n.nextProducer=a:At.producers=a,o&&Lv(t,a)}function Nv(){_m++}function Cr(t){if(!(Do(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===_m)){if(!t.producerMustRecompute(t)&&!Co(t)){wo(t);return}t.producerRecomputeValue(t),wo(t)}}function vm(t){if(t.consumers===void 0)return;let n=lc;lc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||jI(i)}}finally{lc=n}}function bm(){return At?.consumerAllowSignalWrites!==!1}function jI(t){t.dirty=!0,vm(t),t.consumerMarkedDirty?.(t)}function wo(t){t.dirty=!1,t.lastCleanEpoch=_m}function oi(t){return t&&Fv(t),ne(t)}function Fv(t){t.producersTail=void 0,t.recomputing=!0}function Ai(t,n){ne(n),t&&Pv(t)}function Pv(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Do(t))do e=ym(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Co(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Cr(e),i!==e.version))return!0}return!1}function Oi(t){if(Do(t)){let n=t.producers;for(;n!==void 0;)n=ym(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Lv(t,n){let e=t.consumersTail,i=Do(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Lv(r.producer,r)}function ym(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Do(n)){let o=n.producers;for(;o!==void 0;)o=ym(o)}return e}function Do(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Za(t){BI?.(t)}function VI(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Xa(t,n){return Object.is(t,n)}function Ja(t,n){let e=Object.create(HI);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Cr(e),Ri(e),e.value===Vn)throw e.error;return e.value};return i[Xe]=e,Za(e),i}var yr=Symbol("UNSET"),wr=Symbol("COMPUTING"),Vn=Symbol("ERRORED"),HI=J(b({},ki),{value:yr,dirty:!0,error:null,equal:Xa,kind:"computed",producerMustRecompute(t){return t.value===yr||t.value===wr},producerRecomputeValue(t){if(t.value===wr)throw new Error("");let n=t.value;t.value=wr;let e=oi(t),i,r=!1;try{i=t.computation(),ne(null),r=n!==yr&&n!==Vn&&i!==Vn&&t.equal(n,i)}catch(o){i=Vn,t.error=o}finally{Ai(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function zI(){throw new Error}var Bv=zI;function jv(t){Bv(t)}function wm(t){Bv=t}var UI=null;function Cm(t,n){let e=Object.create(es);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Vv(e);return i[Xe]=e,Za(e),[i,a=>Dr(e,a),a=>dc(e,a)]}function Vv(t){return Ri(t),t.value}function Dr(t,n){bm()||jv(t),t.equal(t.value,n)||(t.value=n,$I(t))}function dc(t,n){bm()||jv(t),Dr(t,n(t.value))}var es=J(b({},ki),{equal:Xa,value:void 0,kind:"signal"});function $I(t){t.version++,Nv(),vm(t),UI?.(t)}var Dm=J(b({},ki),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function xm(t){if(t.dirty=!1,t.version>0&&!Co(t))return;t.version++;let n=oi(t);try{t.cleanup(),t.fn()}finally{Ai(t,n)}}function pe(t){return typeof t=="function"}function xo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var uc=xo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function xr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var be=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(pe(i))try{i()}catch(o){n=o instanceof uc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Hv(o)}catch(a){n=n??[],a instanceof uc?n=[...n,...a.errors]:n.push(a)}}if(n)throw new uc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Hv(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&xr(e,n)}remove(n){let{_finalizers:e}=this;e&&xr(e,n),n instanceof t&&n._removeParent(this)}};be.EMPTY=(()=>{let t=new be;return t.closed=!0,t})();var Em=be.EMPTY;function fc(t){return t instanceof be||t&&"closed"in t&&pe(t.remove)&&pe(t.add)&&pe(t.unsubscribe)}function Hv(t){pe(t)?t():t.unsubscribe()}var _n={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Eo={setTimeout(t,n,...e){let{delegate:i}=Eo;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Eo;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function mc(t){Eo.setTimeout(()=>{let{onUnhandledError:n}=_n;if(n)n(t);else throw t})}function Er(){}var zv=Im("C",void 0,void 0);function Uv(t){return Im("E",void 0,t)}function $v(t){return Im("N",t,void 0)}function Im(t,n,e){return{kind:t,value:n,error:e}}var Ir=null;function Io(t){if(_n.useDeprecatedSynchronousErrorHandling){let n=!Ir;if(n&&(Ir={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Ir;if(Ir=null,e)throw i}}else t()}function Gv(t){_n.useDeprecatedSynchronousErrorHandling&&Ir&&(Ir.errorThrown=!0,Ir.error=t)}var Sr=class extends be{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,fc(n)&&n.add(this)):this.destination=qI}static create(n,e,i){return new ai(n,e,i)}next(n){this.isStopped?Mm($v(n),this):this._next(n)}error(n){this.isStopped?Mm(Uv(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Mm(zv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},GI=Function.prototype.bind;function Sm(t,n){return GI.call(t,n)}var Tm=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){hc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){hc(i)}else hc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){hc(e)}}},ai=class extends Sr{constructor(n,e,i){super();let r;if(pe(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&_n.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Sm(n.next,o),error:n.error&&Sm(n.error,o),complete:n.complete&&Sm(n.complete,o)}):r=n}this.destination=new Tm(r)}};function hc(t){_n.useDeprecatedSynchronousErrorHandling?Gv(t):mc(t)}function WI(t){throw t}function Mm(t,n){let{onStoppedNotification:e}=_n;e&&Eo.setTimeout(()=>e(t,n))}var qI={closed:!0,next:Er,error:WI,complete:Er};var So=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Pt(t){return t}function km(...t){return Rm(t)}function Rm(t){return t.length===0?Pt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var oe=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=YI(e)?e:new ai(e,i,r);return Io(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Wv(i),new i((r,o)=>{let a=new ai({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[So](){return this}pipe(...e){return Rm(e)(this)}toPromise(e){return e=Wv(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function Wv(t){var n;return(n=t??_n.Promise)!==null&&n!==void 0?n:Promise}function QI(t){return t&&pe(t.next)&&pe(t.error)&&pe(t.complete)}function YI(t){return t&&t instanceof Sr||QI(t)&&fc(t)}function KI(t){return pe(t?.lift)}function ce(t){return n=>{if(KI(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function de(t,n,e,i,r){return new Am(t,n,e,i,r)}var Am=class extends Sr{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var qv=xo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var x=(()=>{class t extends oe{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new pc(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new qv}next(e){Io(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Io(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Io(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Em:(this.currentObservers=null,o.push(e),new be(()=>{this.currentObservers=null,xr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new oe;return e.source=this,e}}return t.create=(n,e)=>new pc(n,e),t})(),pc=class extends x{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Em}};var Je=class extends x{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var ts={now(){return(ts.delegate||Date).now()},delegate:void 0};var vn=class extends x{constructor(n=1/0,e=1/0,i=ts){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var gc=class extends be{constructor(n,e){super()}schedule(n,e=0){return this}};var ns={setInterval(t,n,...e){let{delegate:i}=ns;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=ns;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var _c=class extends gc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return ns.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&ns.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,xr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Mo=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Mo.now=ts.now;var vc=class extends Mo{constructor(n,e=Mo.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var is=new vc(_c),Qv=is;var et=new oe(t=>t.complete());function bc(t){return t&&pe(t.schedule)}function Om(t){return t[t.length-1]}function yc(t){return pe(Om(t))?t.pop():void 0}function Hn(t){return bc(Om(t))?t.pop():void 0}function Yv(t,n){return typeof Om(t)=="number"?t.pop():n}function Zv(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(u){try{c(i.next(u))}catch(f){a(f)}}function l(u){try{c(i.throw(u))}catch(f){a(f)}}function c(u){u.done?o(u.value):r(u.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function Kv(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Mr(t){return this instanceof Mr?(this.v=t,this):new Mr(t)}function Xv(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(_){return function(C){return Promise.resolve(C).then(_,f)}}function s(_,C){i[_]&&(r[_]=function(O){return new Promise(function(B,Y){o.push([_,O,B,Y])>1||l(_,O)})},C&&(r[_]=C(r[_])))}function l(_,C){try{c(i[_](C))}catch(O){g(o[0][3],O)}}function c(_){_.value instanceof Mr?Promise.resolve(_.value.v).then(u,f):g(o[0][2],_)}function u(_){l("next",_)}function f(_){l("throw",_)}function g(_,C){_(C),o.shift(),o.length&&l(o[0][0],o[0][1])}}function Jv(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Kv=="function"?Kv(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var wc=t=>t&&typeof t.length=="number"&&typeof t!="function";function Cc(t){return pe(t?.then)}function Dc(t){return pe(t[So])}function xc(t){return Symbol.asyncIterator&&pe(t?.[Symbol.asyncIterator])}function Ec(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function ZI(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ic=ZI();function Sc(t){return pe(t?.[Ic])}function Mc(t){return Xv(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Mr(e.read());if(r)return yield Mr(void 0);yield yield Mr(i)}}finally{e.releaseLock()}})}function Tc(t){return pe(t?.getReader)}function Ne(t){if(t instanceof oe)return t;if(t!=null){if(Dc(t))return XI(t);if(wc(t))return JI(t);if(Cc(t))return eS(t);if(xc(t))return eb(t);if(Sc(t))return tS(t);if(Tc(t))return nS(t)}throw Ec(t)}function XI(t){return new oe(n=>{let e=t[So]();if(pe(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function JI(t){return new oe(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function eS(t){return new oe(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,mc)})}function tS(t){return new oe(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function eb(t){return new oe(n=>{iS(t,n).catch(e=>n.error(e))})}function nS(t){return eb(Mc(t))}function iS(t,n){var e,i,r,o;return Zv(this,void 0,void 0,function*(){try{for(e=Jv(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Wt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function kc(t,n=0){return ce((e,i)=>{e.subscribe(de(i,r=>Wt(i,t,()=>i.next(r),n),()=>Wt(i,t,()=>i.complete(),n),r=>Wt(i,t,()=>i.error(r),n)))})}function Rc(t,n=0){return ce((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function tb(t,n){return Ne(t).pipe(Rc(n),kc(n))}function nb(t,n){return Ne(t).pipe(Rc(n),kc(n))}function ib(t,n){return new oe(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function rb(t,n){return new oe(e=>{let i;return Wt(e,n,()=>{i=t[Ic](),Wt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>pe(i?.return)&&i.return()})}function Ac(t,n){if(!t)throw new Error("Iterable cannot be null");return new oe(e=>{Wt(e,n,()=>{let i=t[Symbol.asyncIterator]();Wt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function ob(t,n){return Ac(Mc(t),n)}function ab(t,n){if(t!=null){if(Dc(t))return tb(t,n);if(wc(t))return ib(t,n);if(Cc(t))return nb(t,n);if(xc(t))return Ac(t,n);if(Sc(t))return rb(t,n);if(Tc(t))return ob(t,n)}throw Ec(t)}function Pe(t,n){return n?ab(t,n):Ne(t)}function Z(...t){let n=Hn(t);return Pe(t,n)}function rs(t,n){let e=pe(t)?t:()=>t,i=r=>r.error(e());return new oe(n?r=>n.schedule(i,0,r):i)}function os(t){return!!t&&(t instanceof oe||pe(t.lift)&&pe(t.subscribe))}var Tr=xo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function sb(t){return t instanceof Date&&!isNaN(t)}function N(t,n){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:rS}=Array;function oS(t,n){return rS(n)?t(...n):t(n)}function Oc(t){return N(n=>oS(t,n))}var{isArray:aS}=Array,{getPrototypeOf:sS,prototype:lS,keys:cS}=Object;function Nc(t){if(t.length===1){let n=t[0];if(aS(n))return{args:n,keys:null};if(dS(n)){let e=cS(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function dS(t){return t&&typeof t=="object"&&sS(t)===lS}function Fc(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function kr(...t){let n=Hn(t),e=yc(t),{args:i,keys:r}=Nc(t);if(i.length===0)return Pe([],n);let o=new oe(uS(i,n,r?a=>Fc(r,a):Pt));return e?o.pipe(Oc(e)):o}function uS(t,n,e=Pt){return i=>{lb(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)lb(n,()=>{let c=Pe(t[l],n),u=!1;c.subscribe(de(i,f=>{o[l]=f,u||(u=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function lb(t,n,e){t?Wt(e,t,n):n()}function cb(t,n,e,i,r,o,a,s){let l=[],c=0,u=0,f=!1,g=()=>{f&&!l.length&&!c&&n.complete()},_=O=>c<i?C(O):l.push(O),C=O=>{o&&n.next(O),c++;let B=!1;Ne(e(O,u++)).subscribe(de(n,Y=>{r?.(Y),o?_(Y):n.next(Y)},()=>{B=!0},void 0,()=>{if(B)try{for(c--;l.length&&c<i;){let Y=l.shift();a?Wt(n,a,()=>C(Y)):C(Y)}g()}catch(Y){n.error(Y)}}))};return t.subscribe(de(n,_,()=>{f=!0,g()})),()=>{s?.()}}function Lt(t,n,e=1/0){return pe(n)?Lt((i,r)=>N((o,a)=>n(i,o,r,a))(Ne(t(i,r))),e):(typeof n=="number"&&(e=n),ce((i,r)=>cb(i,r,t,e)))}function Pc(t=1/0){return Lt(Pt,t)}function db(){return Pc(1)}function Ni(...t){return db()(Pe(t,Hn(t)))}function bn(t){return new oe(n=>{Ne(t()).subscribe(n)})}function si(...t){let n=yc(t),{args:e,keys:i}=Nc(t),r=new oe(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let u=0;u<a;u++){let f=!1;Ne(e[u]).subscribe(de(o,g=>{f||(f=!0,c--),s[u]=g},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Fc(i,s):s),o.complete())}))}});return n?r.pipe(Oc(n)):r}function Lc(t=0,n,e=Qv){let i=-1;return n!=null&&(bc(n)?e=n:i=n),new oe(r=>{let o=sb(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Bt(...t){let n=Hn(t),e=Yv(t,1/0),i=t;return i.length?i.length===1?Ne(i[0]):Pc(e)(Pe(i,n)):et}var li=new oe(Er);function ue(t,n){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>t.call(n,o,r++)&&i.next(o)))})}function ub(t){return ce((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(de(e,c=>{i=!0,r=c,o||Ne(t(c)).subscribe(o=de(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Bc(t,n=is){return ub(()=>Lc(t,n))}function zn(t){return ce((n,e)=>{let i=null,r=!1,o;i=n.subscribe(de(e,void 0,void 0,a=>{o=Ne(t(a,zn(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function To(t,n){return pe(n)?Lt(t,n,1):Lt(t,1)}function Rr(t,n=is){return ce((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}s()}e.subscribe(de(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function fb(t){return ce((n,e)=>{let i=!1;n.subscribe(de(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ce(t){return t<=0?()=>et:ce((n,e)=>{let i=0;n.subscribe(de(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function jc(t){return N(()=>t)}function Vc(t,n=Pt){return t=t??fS,ce((e,i)=>{let r,o=!0;e.subscribe(de(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function fS(t,n){return t===n}function mb(t=mS){return ce((n,e)=>{let i=!1;n.subscribe(de(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function mS(){return new Tr}function Fi(t){return ce((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function ci(t,n){let e=arguments.length>=2;return i=>i.pipe(t?ue((r,o)=>t(r,o,i)):Pt,Ce(1),e?fb(n):mb(()=>new Tr))}function Hc(t){return t<=0?()=>et:ce((n,e)=>{let i=[];n.subscribe(de(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function zc(){return ce((t,n)=>{let e,i=!1;t.subscribe(de(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Nm(t=1/0){let n;t&&typeof t=="object"?n=t:n={count:t};let{count:e=1/0,delay:i,resetOnSuccess:r=!1}=n;return e<=0?Pt:ce((o,a)=>{let s=0,l,c=()=>{let u=!1;l=o.subscribe(de(a,f=>{r&&(s=0),a.next(f)},void 0,f=>{if(s++<e){let g=()=>{l?(l.unsubscribe(),l=null,c()):u=!0};if(i!=null){let _=typeof i=="number"?Lc(i):Ne(i(f,s)),C=de(a,()=>{C.unsubscribe(),g()},()=>{a.complete()});_.subscribe(C)}else g()}else a.error(f)})),u&&(l.unsubscribe(),l=null,c())};c()})}function as(t={}){let{connector:n=()=>new x,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,u=!1,f=!1,g=()=>{s?.unsubscribe(),s=void 0},_=()=>{g(),a=l=void 0,u=f=!1},C=()=>{let O=a;_(),O?.unsubscribe()};return ce((O,B)=>{c++,!f&&!u&&g();let Y=l=l??n();B.add(()=>{c--,c===0&&!f&&!u&&(s=Fm(C,r))}),Y.subscribe(B),!a&&c>0&&(a=new ai({next:Se=>Y.next(Se),error:Se=>{f=!0,g(),s=Fm(_,e,Se),Y.error(Se)},complete:()=>{u=!0,g(),s=Fm(_,i),Y.complete()}}),Ne(O).subscribe(a))})(o)}}function Fm(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new ai({next:()=>{i.unsubscribe(),t()}});return Ne(n(...e)).subscribe(i)}function Ar(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,as({connector:()=>new vn(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function ss(t){return ue((n,e)=>t<=e)}function Ge(...t){let n=Hn(t);return ce((e,i)=>{(n?Ni(t,e,n):Ni(t,e)).subscribe(i)})}function Ee(t,n){return ce((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(de(i,l=>{r?.unsubscribe();let c=0,u=o++;Ne(t(l,u)).subscribe(r=de(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function fe(t){return ce((n,e)=>{Ne(t).subscribe(de(e,()=>e.complete(),Er)),!e.closed&&n.subscribe(e)})}function Pm(t,n=!1){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function tt(t,n,e){let i=pe(t)||n||e?{next:t,error:n,complete:e}:t;return i?ce((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(de(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Pt}var Lm;function Uc(){return Lm}function Un(t){let n=Lm;return Lm=t,n}var hb=Symbol("NotFound");function ko(t){return t===hb||t?.name==="\u0275NotFound"}function Bm(t,n,e){let i=Object.create(hS);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Cr(i),Ri(i),i.value===Vn)throw i.error;return i.value};return o[Xe]=i,Za(i),o}function pb(t,n){Cr(t),Dr(t,n),wo(t)}function gb(t,n){if(Cr(t),t.value===Vn)throw t.error;dc(t,n),wo(t)}var hS=J(b({},ki),{value:yr,dirty:!0,error:null,equal:Xa,kind:"linkedSignal",producerMustRecompute(t){return t.value===yr||t.value===wr},producerRecomputeValue(t){if(t.value===wr)throw new Error("");let n=t.value;t.value=wr;let e=oi(t),i,r=!1;try{let o=t.source(),a=n!==yr&&n!==Vn,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ne(null),r=a&&i!==Vn&&t.equal(n,i)}catch(o){i=Vn,t.error=o}finally{Ai(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function _b(t){let n=ne(null);try{return t()}finally{ne(n)}}var Kc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",S=class extends Error{code;constructor(n,e){super(yn(n,e)),this.code=n}};function pS(t){return`NG0${Math.abs(t)}`}function yn(t,n){return`${pS(t)}${n?": "+n:""}`}var ji=globalThis;function Te(t){for(let n in t)if(t[n]===Te)return n;throw Error("")}function Cb(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function hs(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(hs).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Zc(t,n){return t?n?`${t} ${n}`:t:n||""}var gS=Te({__forward_ref__:Te});function wn(t){return t.__forward_ref__=wn,t}function dt(t){return Zm(t)?t():t}function Zm(t){return typeof t=="function"&&t.hasOwnProperty(gS)&&t.__forward_ref__===wn}function w(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function M(t){return{providers:t.providers||[],imports:t.imports||[]}}function ps(t){return _S(t,Xc)}function Xm(t){return ps(t)!==null}function _S(t,n){return t.hasOwnProperty(n)&&t[n]||null}function vS(t){let n=t?.[Xc]??null;return n||null}function Vm(t){return t&&t.hasOwnProperty(Gc)?t[Gc]:null}var Xc=Te({\u0275prov:Te}),Gc=Te({\u0275inj:Te}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Jm(t){return t&&!!t.\u0275providers}var gs=Te({\u0275cmp:Te}),_s=Te({\u0275dir:Te}),eh=Te({\u0275pipe:Te}),th=Te({\u0275mod:Te}),cs=Te({\u0275fac:Te}),Lr=Te({__NG_ELEMENT_ID__:Te}),vb=Te({__NG_ENV_ID__:Te});function nh(t){return ed(t,"@NgModule"),t[th]||null}function ui(t){return ed(t,"@Component"),t[gs]||null}function Jc(t){return ed(t,"@Directive"),t[_s]||null}function Db(t){return ed(t,"@Pipe"),t[eh]||null}function ed(t,n){if(t==null)throw new S(-919,!1)}function Br(t){return typeof t=="string"?t:t==null?"":String(t)}var xb=Te({ngErrorCode:Te}),bS=Te({ngErrorMessage:Te}),yS=Te({ngTokenPath:Te});function ih(t,n){return Eb("",-200,n)}function td(t,n){throw new S(-201,!1)}function Eb(t,n,e){let i=new S(n,t);return i[xb]=n,i[bS]=t,e&&(i[yS]=e),i}function wS(t){return t[xb]}var Hm;function Ib(){return Hm}function jt(t){let n=Hm;return Hm=t,n}function rh(t,n,e){let i=ps(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;td(t,"")}var CS={},Or=CS,DS="__NG_DI_FLAG__",zm=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Nr(e)||0;try{return this.injector.get(n,i&8?null:Or,i)}catch(r){if(ko(r))return r;throw r}}};function xS(t,n=0){let e=Uc();if(e===void 0)throw new S(-203,!1);if(e===null)return rh(t,void 0,n);{let i=ES(n),r=e.retrieve(t,i);if(ko(r)){if(i.optional)return null;throw r}return r}}function V(t,n=0){return(Ib()||xS)(dt(t),n)}function d(t,n){return V(t,Nr(n))}function Nr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function ES(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Um(t){let n=[];for(let e=0;e<t.length;e++){let i=dt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new S(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=IS(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(V(r,o))}else n.push(V(i))}return n}function IS(t){return t[DS]}function Pi(t,n){let e=t.hasOwnProperty(cs);return e?t[cs]:null}function Sb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Mb(t){return t.flat(Number.POSITIVE_INFINITY)}function nd(t,n){t.forEach(e=>Array.isArray(e)?nd(e,n):n(e))}function oh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function vs(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Tb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function kb(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function id(t,n,e){let i=Ao(t,n);return i>=0?t[i|1]=e:(i=~i,kb(t,i,n,e)),i}function rd(t,n){let e=Ao(t,n);if(e>=0)return t[e|1]}function Ao(t,n){return SS(t,n,1)}function SS(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Cn={},Ot=[],jr=new y(""),ah=new y("",-1),sh=new y(""),ds=class{get(n,e=Or){if(e===Or){let r=Eb("",-201);throw r.name="\u0275NotFound",r}return e}};function xt(t){return{\u0275providers:t}}function od(...t){return{\u0275providers:lh(!0,t),\u0275fromNgModule:!0}}function lh(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return nd(n,a=>{let s=a;Wc(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&Rb(r,o),e}function Rb(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];ch(r,o=>{n(o,i)})}}function Wc(t,n,e,i){if(t=dt(t),!t)return!1;let r=null,o=Vm(t),a=!o&&ui(t);if(!o&&!a){let l=t.ngModule;if(o=Vm(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)Wc(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;nd(o.imports,u=>{Wc(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&Rb(c,n)}if(!s){let c=Pi(r)||(()=>new r);n({provide:r,useFactory:c,deps:Ot},r),n({provide:sh,useValue:r,multi:!0},r),n({provide:jr,useValue:()=>V(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;ch(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function ch(t,n){for(let e of t)Jm(e)&&(e=e.\u0275providers),Array.isArray(e)?ch(e,n):n(e)}var MS=Te({provide:String,useValue:Te});function Ab(t){return t!==null&&typeof t=="object"&&MS in t}function TS(t){return!!(t&&t.useExisting)}function kS(t){return!!(t&&t.useFactory)}function Fr(t){return typeof t=="function"}function Ob(t){return!!t.useClass}var bs=new y(""),$c={},bb={},jm;function Oo(){return jm===void 0&&(jm=new ds),jm}var Fe=class{},Pr=class extends Fe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Gm(n,a=>this.processProvider(a)),this.records.set(ah,Ro(void 0,this)),r.has("environment")&&this.records.set(Fe,Ro(void 0,this));let o=this.records.get(bs);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(sh,Ot,{self:!0}))}retrieve(n,e){let i=Nr(e)||0;try{return this.get(n,Or,i)}catch(r){if(ko(r))return r;throw r}}destroy(){ls(this),this._destroyed=!0;let n=ne(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ne(n)}}onDestroy(n){return ls(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ls(this);let e=Un(this),i=jt(void 0),r;try{return n()}finally{Un(e),jt(i)}}get(n,e=Or,i){if(ls(this),n.hasOwnProperty(vb))return n[vb](this);let r=Nr(i),o,a=Un(this),s=jt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=FS(n)&&ps(n);u&&this.injectableDefInScope(u)?c=Ro($m(n),$c):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Oo():this.parent;return e=r&8&&e===Or?null:e,l.get(n,e)}catch(l){let c=wS(l);throw c===-200||c===-201?new S(c,null):l}finally{jt(s),Un(a)}}resolveInjectorInitializers(){let n=ne(null),e=Un(this),i=jt(void 0),r;try{let o=this.get(jr,Ot,{self:!0});for(let a of o)a()}finally{Un(e),jt(i),ne(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=dt(n);let e=Fr(n)?n:dt(n&&n.provide),i=AS(n);if(!Fr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Ro(void 0,$c,!0),r.factory=()=>Um(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ne(null);try{if(e.value===bb)throw ih("");return e.value===$c&&(e.value=bb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&NS(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ne(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=dt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function $m(t){let n=ps(t),e=n!==null?n.factory:Pi(t);if(e!==null)return e;if(t instanceof y)throw new S(-204,!1);if(t instanceof Function)return RS(t);throw new S(-204,!1)}function RS(t){if(t.length>0)throw new S(-204,!1);let e=vS(t);return e!==null?()=>e.factory(t):()=>new t}function AS(t){if(Ab(t))return Ro(void 0,t.useValue);{let n=dh(t);return Ro(n,$c)}}function dh(t,n,e){let i;if(Fr(t)){let r=dt(t);return Pi(r)||$m(r)}else if(Ab(t))i=()=>dt(t.useValue);else if(kS(t))i=()=>t.useFactory(...Um(t.deps||[]));else if(TS(t))i=(r,o)=>V(dt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=dt(t&&(t.useClass||t.provide));if(OS(t))i=()=>new r(...Um(t.deps));else return Pi(r)||$m(r)}return i}function ls(t){if(t.destroyed)throw new S(-205,!1)}function Ro(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function OS(t){return!!t.deps}function NS(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function FS(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Gm(t,n){for(let e of t)Array.isArray(e)?Gm(e,n):e&&Jm(e)?Gm(e.\u0275providers,n):n(e)}function ft(t,n){let e;t instanceof Pr?(ls(t),e=t):e=new zm(t);let i,r=Un(e),o=jt(void 0);try{return n()}finally{Un(r),jt(o)}}function Nb(){return Ib()!==void 0||Uc()!=null}var Dn=0,ie=1,le=2,ut=3,nn=4,Ht=5,Vr=6,No=7,nt=8,fi=9,xn=10,Le=11,Fo=12,uh=13,Hr=14,zt=15,Vi=16,zr=17,Gn=18,mi=19,fh=20,di=21,ad=22,Li=23,Zt=24,Ur=25,Hi=26,We=27,Fb=1,mh=6,zi=7,ys=8,$r=9,Ye=10;function hi(t){return Array.isArray(t)&&typeof t[Fb]=="object"}function En(t){return Array.isArray(t)&&t[Fb]===!0}function hh(t){return(t.flags&4)!==0}function Wn(t){return t.componentOffset>-1}function Po(t){return(t.flags&1)===1}function In(t){return!!t.template}function Lo(t){return(t[le]&512)!==0}function Gr(t){return(t[le]&256)===256}var ph="svg",Pb="math";function rn(t){for(;Array.isArray(t);)t=t[Dn];return t}function gh(t,n){return rn(n[t])}function on(t,n){return rn(n[t.index])}function sd(t,n){return t.data[n]}function _h(t,n){return t[n]}function vh(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function an(t,n){let e=n[t];return hi(e)?e:e[Dn]}function Lb(t){return(t[le]&4)===4}function ld(t){return(t[le]&128)===128}function Bb(t){return En(t[ut])}function Xt(t,n){return n==null?null:t[n]}function bh(t){t[zr]=0}function yh(t){t[le]&1024||(t[le]|=1024,ld(t)&&Wr(t))}function jb(t,n){for(;t>0;)n=n[Hr],t--;return n}function ws(t){return!!(t[le]&9216||t[Zt]?.dirty)}function cd(t){t[xn].changeDetectionScheduler?.notify(8),t[le]&64&&(t[le]|=1024),ws(t)&&Wr(t)}function Wr(t){t[xn].changeDetectionScheduler?.notify(0);let n=Bi(t);for(;n!==null&&!(n[le]&8192||(n[le]|=8192,!ld(n)));)n=Bi(n)}function wh(t,n){if(Gr(t))throw new S(911,!1);t[di]===null&&(t[di]=[]),t[di].push(n)}function Vb(t,n){if(t[di]===null)return;let e=t[di].indexOf(n);e!==-1&&t[di].splice(e,1)}function Bi(t){let n=t[ut];return En(n)?n[ut]:n}function Ch(t){return t[No]??=[]}function Dh(t){return t.cleanup??=[]}function Hb(t,n,e,i){let r=Ch(n);r.push(e),t.firstCreatePass&&Dh(t).push(i,r.length-1)}var ge={lFrame:Jb(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Wm=!1;function zb(){return ge.lFrame.elementDepthCount}function Ub(){ge.lFrame.elementDepthCount++}function xh(){ge.lFrame.elementDepthCount--}function dd(){return ge.bindingsEnabled}function Eh(){return ge.skipHydrationRootTNode!==null}function Ih(t){return ge.skipHydrationRootTNode===t}function Sh(){ge.skipHydrationRootTNode=null}function ae(){return ge.lFrame.lView}function ze(){return ge.lFrame.tView}function _e(t){return ge.lFrame.contextLView=t,t[nt]}function ve(t){return ge.lFrame.contextLView=null,t}function vt(){let t=Mh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Mh(){return ge.lFrame.currentTNode}function $b(){let t=ge.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Bo(t,n){let e=ge.lFrame;e.currentTNode=t,e.isParent=n}function Th(){return ge.lFrame.isParent}function kh(){ge.lFrame.isParent=!1}function Gb(){return ge.lFrame.contextLView}function Rh(){return Wm}function us(t){let n=Wm;return Wm=t,n}function Ah(){let t=ge.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Wb(){return ge.lFrame.bindingIndex}function qb(t){return ge.lFrame.bindingIndex=t}function Ui(){return ge.lFrame.bindingIndex++}function ud(t){let n=ge.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Qb(){return ge.lFrame.inI18n}function Yb(t,n){let e=ge.lFrame;e.bindingIndex=e.bindingRootIndex=t,fd(n)}function Kb(){return ge.lFrame.currentDirectiveIndex}function fd(t){ge.lFrame.currentDirectiveIndex=t}function Zb(t){let n=ge.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function md(){return ge.lFrame.currentQueryIndex}function Cs(t){ge.lFrame.currentQueryIndex=t}function PS(t){let n=t[ie];return n.type===2?n.declTNode:n.type===1?t[Ht]:null}function Oh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=PS(o),r===null||(o=o[Hr],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ge.lFrame=Xb();return i.currentTNode=n,i.lView=t,!0}function hd(t){let n=Xb(),e=t[ie];ge.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Xb(){let t=ge.lFrame,n=t===null?null:t.child;return n===null?Jb(t):n}function Jb(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function ey(){let t=ge.lFrame;return ge.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Nh=ey;function pd(){let t=ey();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function ty(t){return(ge.lFrame.contextLView=jb(t,ge.lFrame.contextLView))[nt]}function qn(){return ge.lFrame.selectedIndex}function $i(t){ge.lFrame.selectedIndex=t}function Ds(){let t=ge.lFrame;return sd(t.tView,t.selectedIndex)}function qt(){ge.lFrame.currentNamespace=ph}function xs(){LS()}function LS(){ge.lFrame.currentNamespace=null}function Fh(){return ge.lFrame.currentNamespace}var ny=!0;function gd(){return ny}function Es(t){ny=t}function qm(t,n=null,e=null,i){let r=Ph(t,n,e,i);return r.resolveInjectorInitializers(),r}function Ph(t,n=null,e=null,i,r=new Set){let o=[e||Ot,od(t)],a;return new Pr(o,n||Oo(),a||null,r)}var j=class t{static THROW_IF_NOT_FOUND=Or;static NULL=new ds;static create(n,e){if(Array.isArray(n))return qm({name:""},e,n,"");{let i=n.name??"";return qm({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:t,providedIn:"any",factory:()=>V(ah)});static __NG_ELEMENT_ID__=-1},K=new y(""),it=(()=>{class t{static __NG_ELEMENT_ID__=BS;static __NG_ENV_ID__=e=>e}return t})(),qc=class extends it{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Gr(this._lView)}onDestroy(n){let e=this._lView;return wh(e,n),()=>Vb(e,n)}};function BS(){return new qc(ae())}var iy=!1,ry=new y(""),pi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Je(!1);debugTaskTracker=d(ry,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new oe(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Qm=class extends x{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Nb()&&(this.destroyRef=d(it,{optional:!0})??void 0,this.pendingTasks=d(pi,{optional:!0})??void 0)}emit(n){let e=ne(null);try{super.next(n)}finally{ne(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof be&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},X=Qm;function Qc(...t){}function Lh(t){let n,e;function i(){t=Qc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function oy(t){return queueMicrotask(()=>t()),()=>{t=Qc}}var Bh="isAngularZone",fs=Bh+"_ID",jS=0,G=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new X(!1);onMicrotaskEmpty=new X(!1);onStable=new X(!1);onError=new X(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=iy}=n;if(typeof Zone>"u")throw new S(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,zS(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Bh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new S(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new S(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,VS,Qc,Qc);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},VS={};function jh(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function HS(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Lh(()=>{t.callbackScheduled=!1,Ym(t),t.isCheckStableRunning=!0,jh(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Ym(t)}function zS(t){let n=()=>{HS(t)},e=jS++;t._inner=t._inner.fork({name:"angular",properties:{[Bh]:!0,[fs]:e,[fs+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(US(l))return i.invokeTask(o,a,s,l);try{return yb(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),wb(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return yb(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!$S(l)&&n(),wb(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,Ym(t),jh(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function Ym(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function yb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function wb(t){t._nesting--,jh(t)}var ms=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new X;onMicrotaskEmpty=new X;onStable=new X;onError=new X;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function US(t){return ay(t,"__ignore_ng_zone__")}function $S(t){return ay(t,"__scheduler_tick__")}function ay(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Vt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},sn=new y("",{factory:()=>{let t=d(G),n=d(Fe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Vt),e.handleError(i))})}}}),sy={provide:jr,useValue:()=>{let t=d(Vt,{optional:!0})},multi:!0};function F(t,n){let[e,i,r]=Cm(t,n?.equal),o=e,a=o[Xe];return o.set=i,o.update=r,o.asReadonly=_d.bind(o),o}function _d(){let t=this[Xe];if(t.readonlyFn===void 0){let n=()=>this();n[Xe]=t,t.readonlyFn=n}return t.readonlyFn}var jo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=GS}return t})();function GS(){return new jo(ae(),vt())}var $n=class{},Is=new y("",{factory:()=>!0});var Vh=new y(""),qr=(()=>{class t{internalPendingTasks=d(pi);scheduler=d($n);errorHandler=d(sn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),vd=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>new Km})}return t})(),Km=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Yc=class{[Xe];constructor(n){this[Xe]=n}destroy(){this[Xe].destroy()}};function Gi(t,n){let e=n?.injector??d(j),i=n?.manualCleanup!==!0?e.get(it):null,r,o=e.get(jo,null,{optional:!0}),a=e.get($n);return o!==null?(r=QS(o.view,a,t),i instanceof qc&&i._lView===o.view&&(i=null)):r=YS(t,e.get(vd),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Yc(r)}var ly=J(b({},Dm),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=us(!1);try{xm(this)}finally{us(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ne(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ne(t)}}}),WS=J(b({},ly),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Oi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),qS=J(b({},ly),{consumerMarkedDirty(){this.view[le]|=8192,Wr(this.view),this.notifier.notify(13)},destroy(){if(Oi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Li]?.delete(this)}});function QS(t,n,e){let i=Object.create(qS);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=cy(i,e),t[Li]??=new Set,t[Li].add(i),i.consumerMarkedDirty(i),i}function YS(t,n,e){let i=Object.create(WS);return i.fn=cy(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function cy(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Ps(t){return{toString:t}.toString()}function nM(t){return typeof t=="function"}function Wy(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Md=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},qe=(()=>{let t=()=>qy;return t.ngInherit=!0,t})();function qy(t){return t.type.prototype.ngOnChanges&&(t.setInput=rM),iM}function iM(){let t=Yy(this),n=t?.current;if(n){let e=t.previous;if(e===Cn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function rM(t,n,e,i,r){let o=this.declaredInputs[i],a=Yy(t)||oM(t,{previous:Cn,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Md(c&&c.currentValue,e,l===Cn),Wy(t,n,r,e)}var Qy="__ngSimpleChanges__";function Yy(t){return t[Qy]||null}function oM(t,n){return t[Qy]=n}var dy=[];var ke=function(t,n=null,e){for(let i=0;i<dy.length;i++){let r=dy[i];r(t,n,e)}},xe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(xe||{});function aM(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=qy(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Ky(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Dd(t,n,e){Zy(t,n,3,e)}function xd(t,n,e,i){(t[le]&3)===e&&Zy(t,n,e,i)}function Hh(t,n){let e=t[le];(e&3)===n&&(e&=16383,e+=1,t[le]=e)}function Zy(t,n,e,i){let r=i!==void 0?t[zr]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[zr]+=65536),(s<o||o==-1)&&(sM(t,e,n,l),t[zr]=(t[zr]&4294901760)+l+2),l++}function uy(t,n){ke(xe.LifecycleHookStart,t,n);let e=ne(null);try{n.call(t)}finally{ne(e),ke(xe.LifecycleHookEnd,t,n)}}function sM(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[le]>>14<t[zr]>>16&&(t[le]&3)===n&&(t[le]+=16384,uy(s,o)):uy(s,o)}var Ho=-1,Yr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function lM(t){return(t.flags&8)!==0}function cM(t){return(t.flags&16)!==0}function dM(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];uM(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function Xy(t){return t===3||t===4||t===6}function uM(t){return t.charCodeAt(0)===64}function zo(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?fy(t,e,r,null,n[++i]):fy(t,e,r,null,null))}}return t}function fy(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Jy(t){return t!==Ho}function Td(t){return t&32767}function fM(t){return t>>16}function kd(t,n){let e=fM(t),i=n;for(;e>0;)i=i[Hr],e--;return i}var Zh=!0;function Rd(t){let n=Zh;return Zh=t,n}var mM=256,e0=mM-1,t0=5,hM=0,Qn={};function pM(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Lr)&&(i=e[Lr]),i==null&&(i=e[Lr]=hM++);let r=i&e0,o=1<<r;n.data[t+(r>>t0)]|=o}function Ad(t,n){let e=n0(t,n);if(e!==-1)return e;let i=n[ie];i.firstCreatePass&&(t.injectorIndex=n.length,zh(i.data,t),zh(n,null),zh(i.blueprint,null));let r=Fp(t,n),o=t.injectorIndex;if(Jy(r)){let a=Td(r),s=kd(r,n),l=s[ie].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function zh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function n0(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Fp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=s0(r),i===null)return Ho;if(e++,r=r[Hr],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ho}function Xh(t,n,e){pM(t,n,e)}function gM(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Xy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function i0(t,n,e){if(e&8||t!==void 0)return t;td(n,"NodeInjector")}function r0(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[fi],o=jt(void 0);try{return r?r.get(n,i,e&8):rh(n,i,e&8)}finally{jt(o)}}return i0(i,n,e)}function o0(t,n,e,i=0,r){if(t!==null){if(n[le]&2048&&!(i&2)){let a=yM(t,n,e,i,Qn);if(a!==Qn)return a}let o=a0(t,n,e,i,Qn);if(o!==Qn)return o}return r0(n,e,i,r)}function a0(t,n,e,i,r){let o=vM(e);if(typeof o=="function"){if(!Oh(n,t,i))return i&1?i0(r,e,i):r0(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))td(e);else return a}finally{Nh()}}else if(typeof o=="number"){let a=null,s=n0(t,n),l=Ho,c=i&1?n[zt][Ht]:null;for((s===-1||i&4)&&(l=s===-1?Fp(t,n):n[s+8],l===Ho||!hy(i,!1)?s=-1:(a=n[ie],s=Td(l),n=kd(l,n)));s!==-1;){let u=n[ie];if(my(o,s,u.data)){let f=_M(s,n,e,a,i,c);if(f!==Qn)return f}l=n[s+8],l!==Ho&&hy(i,n[ie].data[s+8]===c)&&my(o,s,n)?(a=u,s=Td(l),n=kd(l,n)):s=-1}}return r}function _M(t,n,e,i,r,o){let a=n[ie],s=a.data[t+8],l=i==null?Wn(s)&&Zh:i!=a&&(s.type&3)!==0,c=r&1&&o===s,u=Ed(s,a,e,l,c);return u!==null?ks(n,a,u,s,r):Qn}function Ed(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?s:s+u,g=r?s+u:c;for(let _=f;_<g;_++){let C=a[_];if(_<l&&e===C||_>=l&&C.type===e)return _}if(r){let _=a[l];if(_&&In(_)&&_.type===e)return l}return null}function ks(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof Yr){let s=o;if(s.resolving)throw ih("");let l=Rd(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],u,f=s.injectImpl?jt(s.injectImpl):null,g=Oh(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&aM(e,a[e],n)}finally{f!==null&&jt(f),Rd(l),s.resolving=!1,Nh()}}return o}function vM(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Lr)?t[Lr]:void 0;return typeof n=="number"?n>=0?n&e0:bM:n}function my(t,n,e){let i=1<<t;return!!(e[n+(t>>t0)]&i)}function hy(t,n){return!(t&2)&&!(t&1&&n)}var Qr=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return o0(this._tNode,this._lView,n,Nr(i),e)}};function bM(){return new Qr(vt(),ae())}function Qe(t){return Ps(()=>{let n=t.prototype.constructor,e=n[cs]||Jh(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[cs]||Jh(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Jh(t){return Zm(t)?()=>{let n=Jh(dt(t));return n&&n()}:Pi(t)}function yM(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[le]&2048&&!Lo(a);){let s=a0(o,a,e,i|2,Qn);if(s!==Qn)return s;let l=o.parent;if(!l){let c=a[fh];if(c){let u=c.get(e,Qn,i&-5);if(u!==Qn)return u}l=s0(a),a=a[Hr]}o=l}return r}function s0(t){let n=t[ie],e=n.type;return e===2?n.declTNode:e===1?t[Ht]:null}function Ls(t){return gM(vt(),t)}function wM(){return qo(vt(),ae())}function qo(t,n){return new L(on(t,n))}var L=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=wM}return t})();function l0(t){return t instanceof L?t.nativeElement:t}function CM(){return this._results[Symbol.iterator]()}var Tn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new x}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Mb(n);(this._changesDetected=!Sb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=CM};function c0(t){return(t.flags&128)===128}var Pp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Pp||{}),d0=new Map,DM=0;function xM(){return DM++}function EM(t){d0.set(t[mi],t)}function ep(t){d0.delete(t[mi])}var py="__ngContext__";function Uo(t,n){hi(n)?(t[py]=n[mi],EM(n)):t[py]=n}function u0(t){return m0(t[Fo])}function f0(t){return m0(t[nn])}function m0(t){for(;t!==null&&!En(t);)t=t[nn];return t}var tp;function Lp(t){tp=t}function h0(){if(tp!==void 0)return tp;if(typeof document<"u")return document;throw new S(210,!1)}var qi=new y("",{factory:()=>IM}),IM="ng";var Gd=new y(""),Jr=new y("",{providedIn:"platform",factory:()=>"unknown"}),Bs=new y(""),eo=new y("",{factory:()=>d(K).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var p0="r";var g0="di";var _0=!1,v0=new y("",{factory:()=>_0});var gy=new WeakMap;function SM(t,n){if(t==null||typeof t!="object")return;let e=gy.get(t);e||(e=new WeakSet,gy.set(t,e)),e.add(n)}var MM=(t,n,e,i)=>{};function TM(t,n,e,i){MM(t,n,e,i)}function Wd(t){return(t.flags&32)===32}var kM=()=>null;function b0(t,n,e=!1){return kM(t,n,e)}function y0(t,n){let e=t.contentQueries;if(e!==null){let i=ne(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];Cs(o),s.contentQueries(2,n[a],a)}}}finally{ne(i)}}}function np(t,n,e){Cs(0);let i=ne(null);try{n(t,e)}finally{ne(i)}}function Bp(t,n,e){if(hh(n)){let i=ne(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{ne(i)}}}var kn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(kn||{});var bd;function RM(){if(bd===void 0&&(bd=null,ji.trustedTypes))try{bd=ji.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return bd}function qd(t){return RM()?.createHTML(t)||t}var yd;function w0(){if(yd===void 0&&(yd=null,ji.trustedTypes))try{yd=ji.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return yd}function _y(t){return w0()?.createHTML(t)||t}function vy(t){return w0()?.createScriptURL(t)||t}var gi=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Kc})`}},ip=class extends gi{getTypeName(){return"HTML"}},rp=class extends gi{getTypeName(){return"Style"}},op=class extends gi{getTypeName(){return"Script"}},ap=class extends gi{getTypeName(){return"URL"}},sp=class extends gi{getTypeName(){return"ResourceURL"}};function cn(t){return t instanceof gi?t.changingThisBreaksApplicationSecurity:t}function Zn(t,n){let e=C0(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Kc})`)}return e===n}function C0(t){return t instanceof gi&&t.getTypeName()||null}function jp(t){return new ip(t)}function Vp(t){return new rp(t)}function Hp(t){return new op(t)}function zp(t){return new ap(t)}function Up(t){return new sp(t)}function AM(t){let n=new cp(t);return OM()?new lp(n):n}var lp=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(qd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},cp=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=qd(n),e}};function OM(){try{return!!new window.DOMParser().parseFromString(qd(""),"text/html")}catch{return!1}}var NM=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function js(t){return t=String(t),t.match(NM)?t:"unsafe:"+t}function _i(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Vs(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var D0=_i("area,br,col,hr,img,wbr"),x0=_i("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),E0=_i("rp,rt"),FM=Vs(E0,x0),PM=Vs(x0,_i("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),LM=Vs(E0,_i("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),by=Vs(D0,PM,LM,FM),I0=_i("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),BM=_i("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),jM=_i("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),VM=Vs(I0,BM,jM),HM=_i("script,style,template"),dp=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=$M(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=UM(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=yy(n).toLowerCase();if(!by.hasOwnProperty(e))return this.sanitizedSomething=!0,!HM.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!VM.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;I0[s]&&(l=js(l)),this.buf.push(" ",a,'="',wy(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=yy(n).toLowerCase();by.hasOwnProperty(e)&&!D0.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(wy(n))}};function zM(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function UM(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw S0(n);return n}function $M(t){let n=t.firstChild;if(n&&zM(t,n))throw S0(n);return n}function yy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function S0(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var GM=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,WM=/([^\#-~ |!])/g;function wy(t){return t.replace(/&/g,"&amp;").replace(GM,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(WM,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var wd;function Qd(t,n){let e=null;try{wd=wd||AM(t);let i=n?String(n):"";e=wd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=wd.getInertBodyElement(i)}while(i!==o);let s=new dp().sanitizeChildren(Cy(e)||e);return qd(s)}finally{if(e){let i=Cy(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Cy(t){return"content"in t&&qM(t)?t.content:null}function qM(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var QM=/^>|^->|<!--|-->|--!>|<!-$/g,YM=/(<|>)/g,KM="\u200B$1\u200B";function ZM(t){return t.replace(QM,n=>n.replace(YM,KM))}function XM(t,n){return t.createText(n)}function JM(t,n,e){t.setValue(n,e)}function eT(t,n){return t.createComment(ZM(n))}function M0(t,n,e){return t.createElement(n,e)}function Od(t,n,e,i,r){t.insertBefore(n,e,i,r)}function T0(t,n,e){t.appendChild(n,e)}function Dy(t,n,e,i,r){i!==null?Od(t,n,e,i,r):T0(t,n,e)}function k0(t,n,e,i){t.removeChild(null,n,e,i)}function tT(t,n,e){t.setAttribute(n,"style",e)}function nT(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function R0(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&dM(t,n,i),r!==null&&nT(t,n,r),o!==null&&tT(t,n,o)}var at=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(at||{});function $p(t){let n=Wp();return n?_y(n.sanitize(at.HTML,t)||""):Zn(t,"HTML")?_y(cn(t)):Qd(h0(),Br(t))}function Qo(t){let n=Wp();return n?n.sanitize(at.URL,t)||"":Zn(t,"URL")?cn(t):js(Br(t))}function A0(t){let n=Wp();if(n)return vy(n.sanitize(at.RESOURCE_URL,t)||"");if(Zn(t,"ResourceURL"))return vy(cn(t));throw new S(904,!1)}var iT={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function rT(t,n){return iT[t.toLowerCase()]?.[n.toLowerCase()]===!0?A0:Qo}function Gp(t,n,e){return rT(n,e)(t)}function Wp(){let t=ae();return t&&t[xn].sanitizer}function O0(t){return t instanceof Function?t():t}function oT(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var N0="ng-template";function aT(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&oT(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(qp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function qp(t){return t.type===4&&t.value!==N0}function sT(t,n,e){let i=t.type===4&&!e?N0:t.value;return n===i}function lT(t,n,e){let i=4,r=t.attrs,o=r!==null?uT(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!Sn(i)&&!Sn(l))return!1;if(a&&Sn(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!sT(t,l,e)||l===""&&n.length===1){if(Sn(i))return!1;a=!0}}else if(i&8){if(r===null||!aT(t,r,l,e)){if(Sn(i))return!1;a=!0}}else{let c=n[++s],u=cT(l,r,qp(t),e);if(u===-1){if(Sn(i))return!1;a=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(Sn(i))return!1;a=!0}}}}return Sn(i)||a}function Sn(t){return(t&1)===0}function cT(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return fT(n,t)}function F0(t,n,e=!1){for(let i=0;i<n.length;i++)if(lT(t,n[i],e))return!0;return!1}function dT(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function uT(t){for(let n=0;n<t.length;n++){let e=t[n];if(Xy(e))return n}return t.length}function fT(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function mT(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function xy(t,n){return t?":not("+n.trim()+")":n}function hT(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Sn(a)&&(n+=xy(o,r),r=""),i=a,o=o||!Sn(i);e++}return r!==""&&(n+=xy(o,r)),n}function pT(t){return t.map(hT).join(",")}function gT(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Sn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Qt={};function Qp(t,n,e,i,r,o,a,s,l,c,u){let f=We+i,g=f+r,_=_T(f,g),C=typeof c=="function"?c():c;return _[ie]={type:t,blueprint:_,template:e,queries:null,viewQuery:s,declTNode:n,data:_.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:C,incompleteFirstPass:!1,ssrId:u}}function _T(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Qt);return e}function vT(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Qp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Yp(t,n,e,i,r,o,a,s,l,c,u){let f=n.blueprint.slice();return f[Dn]=r,f[le]=i|4|128|8|64|1024,(c!==null||t&&t[le]&2048)&&(f[le]|=2048),bh(f),f[ut]=f[Hr]=t,f[nt]=e,f[xn]=a||t&&t[xn],f[Le]=s||t&&t[Le],f[fi]=l||t&&t[fi]||null,f[Ht]=o,f[mi]=xM(),f[Vr]=u,f[fh]=c,f[zt]=n.type==2?t[zt]:f,f}function bT(t,n,e){let i=on(n,t),r=vT(e),o=t[xn].rendererFactory,a=Kp(t,Yp(t,r,null,P0(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function P0(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function L0(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Kp(t,n){return t[Fo]?t[uh][nn]=n:t[Fo]=n,t[uh]=n,n}function m(t=1){B0(ze(),ae(),qn()+t,!1)}function B0(t,n,e,i){if(!i)if((n[le]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Dd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&xd(n,o,0,e)}$i(e)}var Yd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Yd||{});function up(t,n,e,i){let r=ne(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Yd.SignalBased)!==0&&(l=n[o][Xe]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):Wy(n,l,o,i)}finally{ne(r)}}var Yn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Yn||{}),yT;function Zp(t,n){return yT(t,n)}var y8=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var fp=new WeakMap,Ss=new WeakSet;function wT(t,n){let e=fp.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),Ss.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function CT(t,n){let e=fp.get(t);e?e.includes(n)||e.push(n):fp.set(t,[n])}var Kr=new Set,Kd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Kd||{}),Rn=new y(""),Ey=new Set;function Qi(t){Ey.has(t)||(Ey.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Zd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Xp=[0,1,2,3],Jp=(()=>{class t{ngZone=d(G);scheduler=d($n);errorHandler=d(Vt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Rn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ke(xe.AfterRenderHooksStart),this.executing=!0;for(let i of Xp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ke(xe.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Ur]??=[]).push(e),Wr(i),i[le]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Kd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Rs=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Ur];n&&(this.view[Ur]=n.filter(e=>e!==this))}};function Be(t,n){let e=n?.injector??d(j);return Qi("NgAfterNextRender"),xT(t,e,n,!0)}function DT(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function xT(t,n,e,i){let r=n.get(Zd);r.impl??=n.get(Jp);let o=n.get(Rn,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(it):null,s=n.get(jo,null,{optional:!0}),l=new Rs(r.impl,DT(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var j0=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Fe)})});function V0(t,n,e){let i=t.get(j0);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function ET(t,n){let e=t.get(j0);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function IT(t,n){for(let[e,i]of n)V0(t,i.animateFns)}function Iy(t,n,e,i){let r=t?.[Hi]?.enter;n!==null&&r&&r.has(e.index)&&IT(i,r)}function Vo(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;En(r)?l=r:hi(r)&&(c=!0,r=r[Dn]);let u=rn(r);t===0&&i!==null?(Iy(s,i,o,e),a==null?T0(n,i,u):Od(n,i,u,a||null,!0)):t===1&&i!==null?(Iy(s,i,o,e),Od(n,i,u,a||null,!0),wT(o,u)):t===2?(s?.[Hi]?.leave?.has(o.index)&&CT(o,u),Ss.delete(u),Sy(s,o,e,f=>{if(Ss.has(u)){Ss.delete(u);return}k0(n,u,c,f)})):t===3&&(Ss.delete(u),Sy(s,o,e,()=>{n.destroyNode(u)})),l!=null&&LT(n,t,e,l,o,i,a)}}function ST(t,n){H0(t,n),n[Dn]=null,n[Ht]=null}function MT(t,n,e,i,r,o){i[Dn]=r,i[Ht]=n,Jd(t,i,e,1,r,o)}function H0(t,n){n[xn].changeDetectionScheduler?.notify(9),Jd(t,n,n[Le],2,null,null)}function TT(t){let n=t[Fo];if(!n)return Uh(t[ie],t);for(;n;){let e=null;if(hi(n))e=n[Fo];else{let i=n[Ye];i&&(e=i)}if(!e){for(;n&&!n[nn]&&n!==t;)hi(n)&&Uh(n[ie],n),n=n[ut];n===null&&(n=t),hi(n)&&Uh(n[ie],n),e=n&&n[nn]}n=e}}function eg(t,n){let e=t[$r],i=e.indexOf(n);e.splice(i,1)}function Xd(t,n){if(Gr(n))return;let e=n[Le];e.destroyNode&&Jd(t,n,e,3,null,null),TT(n)}function Uh(t,n){if(Gr(n))return;let e=ne(null);try{n[le]&=-129,n[le]|=256,n[Zt]&&Oi(n[Zt]),AT(t,n),RT(t,n),n[ie].type===1&&n[Le].destroy();let i=n[Vi];if(i!==null&&En(n[ut])){i!==n[ut]&&eg(i,n);let r=n[Gn];r!==null&&r.detachView(t)}ep(n)}finally{ne(e)}}function Sy(t,n,e,i){let r=t?.[Hi];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Kr.add(t[mi]),V0(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:u}=c();s.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),kT(t,i)}else t&&Kr.delete(t[mi]),i(!1)},r)}function kT(t,n){let e=t[Hi]?.running;if(e){e.then(()=>{t[Hi].running=void 0,Kr.delete(t[mi]),n(!0)});return}n(!1)}function RT(t,n){let e=t.cleanup,i=n[No];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[No]=null);let r=n[di];if(r!==null){n[di]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Li];if(o!==null){n[Li]=null;for(let a of o)a.destroy()}}function AT(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Yr)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];ke(xe.LifecycleHookStart,s,l);try{l.call(s)}finally{ke(xe.LifecycleHookEnd,s,l)}}else{ke(xe.LifecycleHookStart,r,o);try{o.call(r)}finally{ke(xe.LifecycleHookEnd,r,o)}}}}}function z0(t,n,e){return OT(t,n.parent,e)}function OT(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Dn];if(Wn(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===kn.None||r===kn.Emulated)return null}return on(i,e)}function U0(t,n,e){return FT(t,n,e)}function NT(t,n,e){return t.type&40?on(t,e):null}var FT=NT,My;function tg(t,n,e,i){let r=z0(t,i,n),o=n[Le],a=i.parent||n[Ht],s=U0(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)Dy(o,r,e[l],s,!1);else Dy(o,r,e,s,!1);My!==void 0&&My(o,i,n,e,r)}function Ms(t,n){if(n!==null){let e=n.type;if(e&3)return on(n,t);if(e&4)return mp(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Ms(t,i);{let r=t[n.index];return En(r)?mp(-1,r):rn(r)}}else{if(e&128)return Ms(t,n.next);if(e&32)return Zp(n,t)()||rn(t[n.index]);{let i=$0(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Bi(t[zt]);return Ms(r,i)}else return Ms(t,n.next)}}}return null}function $0(t,n){if(n!==null){let i=t[zt][Ht],r=n.projection;return i.projection[r]}return null}function mp(t,n){let e=Ye+t+1;if(e<n.length){let i=n[e],r=i[ie].firstChild;if(r!==null)return Ms(i,r)}return n[zi]}function ng(t,n,e,i,r,o,a){for(;e!=null;){let s=i[fi];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Uo(rn(l),i),e.flags|=2),!Wd(e))if(c&8)ng(t,n,e.child,i,r,o,!1),Vo(n,t,s,r,l,e,o,i);else if(c&32){let u=Zp(e,i),f;for(;f=u();)Vo(n,t,s,r,f,e,o,i);Vo(n,t,s,r,l,e,o,i)}else c&16?G0(t,n,i,e,r,o):Vo(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function Jd(t,n,e,i,r,o){ng(e,i,t.firstChild,n,r,o,!1)}function PT(t,n,e){let i=n[Le],r=z0(t,e,n),o=e.parent||n[Ht],a=U0(o,e,n);G0(i,0,n,e,r,a)}function G0(t,n,e,i,r,o){let a=e[zt],l=a[Ht].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Vo(n,t,e[fi],r,u,i,o,e)}else{let c=l,u=a[ut];c0(i)&&(c.flags|=128),ng(t,n,c,u,r,o,!0)}}function LT(t,n,e,i,r,o,a){let s=i[zi],l=rn(i);s!==l&&Vo(n,t,e,o,s,r,a);for(let c=Ye;c<i.length;c++){let u=i[c];Jd(u[ie],u,t,n,o,s)}}function BT(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Yn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Yn.Important),t.setStyle(e,i,r,o))}}function W0(t,n,e,i,r){let o=qn(),a=i&2;try{$i(-1),a&&n.length>We&&B0(t,n,We,!1);let s=a?xe.TemplateUpdateStart:xe.TemplateCreateStart;ke(s,r,e),e(i,r)}finally{$i(o);let s=a?xe.TemplateUpdateEnd:xe.TemplateCreateEnd;ke(s,r,e)}}function eu(t,n,e){$T(t,n,e),(e.flags&64)===64&&GT(t,n,e)}function Hs(t,n,e=on){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function jT(t,n,e,i){let o=i.get(v0,_0)||e===kn.ShadowDom||e===kn.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return VT(a),a}function VT(t){HT(t)}var HT=()=>null;function zT(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function UT(t,n,e,i,r,o){let a=n[ie];if(tu(t,a,n,e,i)){Wn(t)&&Q0(n,t.index);return}t.type&3&&(e=zT(e)),q0(t,n,e,i,r,o)}function q0(t,n,e,i,r,o){if(t.type&3){let a=on(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function Q0(t,n){let e=an(n,t);e[le]&16||(e[le]|=64)}function $T(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Wn(e)&&bT(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Ad(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=ks(n,t,a,e);if(Uo(l,n),o!==null&&QT(n,a-i,l,s,e,o),In(s)){let c=an(e.index,n);c[nt]=ks(n,t,a,e)}}}function GT(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=Kb();try{$i(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];fd(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&WT(l,c)}}finally{$i(-1),fd(a)}}function WT(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function ig(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];F0(n,o.selectors,!1)&&(i??=[],In(o)?i.unshift(o):i.push(o))}return i}function qT(t,n,e,i,r,o){let a=on(t,n);Y0(n[Le],a,o,t.value,e,i,r)}function Y0(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?Br(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function QT(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];up(i,e,l,c)}}function rg(t,n,e,i,r){let o=We+e,a=n[ie],s=r(a,n,t,i,e);n[o]=s,Bo(t,!0);let l=t.type===2;return l?(R0(n[Le],s,t),(zb()===0||Po(t))&&Uo(s,n),Ub()):Uo(s,n),gd()&&(!l||!Wd(t))&&tg(a,n,s,t),t}function og(t){let n=t;return Th()?kh():(n=n.parent,Bo(n,!1)),n}function YT(t,n){let e=t[fi];if(!e)return;let i;try{i=e.get(sn,null)}catch{i=null}i?.(n)}function tu(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],u=a[l+1],f=n.data[c];up(f,e[c],u,r),s=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];up(u,c,i,r),s=!0}return s}function KT(t,n){let e=an(n,t),i=e[ie];ZT(i,e);let r=e[Dn];r!==null&&e[Vr]===null&&(e[Vr]=b0(r,e[fi])),ke(xe.ComponentStart);try{ag(i,e,e[nt])}finally{ke(xe.ComponentEnd,e[nt])}}function ZT(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function ag(t,n,e){hd(n);try{let i=t.viewQuery;i!==null&&np(1,i,e);let r=t.template;r!==null&&W0(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Gn]?.finishViewCreation(t),t.staticContentQueries&&y0(t,n),t.staticViewQueries&&np(2,t.viewQuery,e);let o=t.components;o!==null&&XT(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[le]&=-5,pd()}}function XT(t,n){for(let e=0;e<n.length;e++)KT(t,n[e])}function zs(t,n,e,i){let r=ne(null);try{let o=n.tView,s=t[le]&4096?4096:16,l=Yp(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Vi]=c;let u=t[Gn];return u!==null&&(l[Gn]=u.createEmbeddedView(o)),ag(o,l,e),l}finally{ne(r)}}function $o(t,n){return!n||n.firstChild===null||c0(t)}function As(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(rn(o)),En(o)&&K0(o,i);let a=e.type;if(a&8)As(t,n,e.child,i);else if(a&32){let s=Zp(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=$0(n,e);if(Array.isArray(s))i.push(...s);else{let l=Bi(n[zt]);As(l[ie],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function K0(t,n){for(let e=Ye;e<t.length;e++){let i=t[e],r=i[ie].firstChild;r!==null&&As(i[ie],i,r,n)}t[zi]!==t[Dn]&&n.push(t[zi])}function Z0(t){if(t[Ur]!==null){for(let n of t[Ur])n.impl.addSequence(n);t[Ur].length=0}}var X0=[];function JT(t){return t[Zt]??ek(t)}function ek(t){let n=X0.pop()??Object.create(nk);return n.lView=t,n}function tk(t){t.lView[Zt]!==t&&(t.lView=null,X0.push(t))}var nk=J(b({},ki),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Wr(t.lView)},consumerOnSignalRead(){this.lView[Zt]=this}});function ik(t){let n=t[Zt]??Object.create(rk);return n.lView=t,n}var rk=J(b({},ki),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Bi(t.lView);for(;n&&!J0(n[ie]);)n=Bi(n);n&&yh(n)},consumerOnSignalRead(){this.lView[Zt]=this}});function J0(t){return t.type!==2}function ew(t){if(t[Li]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Li])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[le]&8192)}}var ok=100;function tw(t,n=0){let i=t[xn].rendererFactory,r=!1;r||i.begin?.();try{ak(t,n)}finally{r||i.end?.()}}function ak(t,n){let e=Rh();try{us(!0),hp(t,n);let i=0;for(;ws(t);){if(i===ok)throw new S(103,!1);i++,hp(t,1)}}finally{us(e)}}function sk(t,n,e,i){if(Gr(n))return;let r=n[le],o=!1,a=!1;hd(n);let s=!0,l=null,c=null;o||(J0(t)?(c=JT(n),l=oi(c)):cc()===null?(s=!1,c=ik(n),l=oi(c)):n[Zt]&&(Oi(n[Zt]),n[Zt]=null));try{bh(n),qb(t.bindingStartIndex),e!==null&&W0(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&Dd(n,_,null)}else{let _=t.preOrderHooks;_!==null&&xd(n,_,0,null),Hh(n,0)}if(a||lk(n),ew(n),nw(n,0),t.contentQueries!==null&&y0(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&Dd(n,_)}else{let _=t.contentHooks;_!==null&&xd(n,_,1),Hh(n,1)}dk(t,n);let f=t.components;f!==null&&rw(n,f,0);let g=t.viewQuery;if(g!==null&&np(2,g,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&Dd(n,_)}else{let _=t.viewHooks;_!==null&&xd(n,_,2),Hh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[ad]){for(let _ of n[ad])_();n[ad]=null}o||(Z0(n),n[le]&=-73)}catch(u){throw o||Wr(n),u}finally{c!==null&&(Ai(c,l),s&&tk(c)),pd()}}function nw(t,n){for(let e=u0(t);e!==null;e=f0(e))for(let i=Ye;i<e.length;i++){let r=e[i];iw(r,n)}}function lk(t){for(let n=u0(t);n!==null;n=f0(n)){if(!(n[le]&2))continue;let e=n[$r];for(let i=0;i<e.length;i++){let r=e[i];yh(r)}}}function ck(t,n,e){ke(xe.ComponentStart);let i=an(n,t);try{iw(i,e)}finally{ke(xe.ComponentEnd,i[nt])}}function iw(t,n){ld(t)&&hp(t,n)}function hp(t,n){let i=t[ie],r=t[le],o=t[Zt],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&Co(o)),a||=!1,o&&(o.dirty=!1),t[le]&=-9217,a)sk(i,t,i.template,t[nt]);else if(r&8192){let s=ne(null);try{ew(t),nw(t,1);let l=i.components;l!==null&&rw(t,l,1),Z0(t)}finally{ne(s)}}}function rw(t,n,e){for(let i=0;i<n.length;i++)ck(t,n[i],e)}function dk(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)$i(~r);else{let o=r,a=e[++i],s=e[++i];Yb(a,o);let l=n[o];ke(xe.HostBindingsUpdateStart,l);try{s(2,l)}finally{ke(xe.HostBindingsUpdateEnd,l)}}}}finally{$i(-1)}}function sg(t,n){let e=Rh()?64:1088;for(t[xn].changeDetectionScheduler?.notify(n);t;){t[le]|=e;let i=Bi(t);if(Lo(t)&&!i)return t;t=i}return null}function ow(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function aw(t,n){let e=Ye+n;if(e<t.length)return t[e]}function Us(t,n,e,i=!0){let r=n[ie];if(uk(r,n,t,e),i){let a=mp(e,t),s=n[Le],l=s.parentNode(t[zi]);l!==null&&MT(r,t[Ht],s,n,l,a)}let o=n[Vr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function sw(t,n){let e=Os(t,n);return e!==void 0&&Xd(e[ie],e),e}function Os(t,n){if(t.length<=Ye)return;let e=Ye+n,i=t[e];if(i){let r=i[Vi];r!==null&&r!==t&&eg(r,i),n>0&&(t[e-1][nn]=i[nn]);let o=vs(t,Ye+n);ST(i[ie],i);let a=o[Gn];a!==null&&a.detachView(o[ie]),i[ut]=null,i[nn]=null,i[le]&=-129}return i}function uk(t,n,e,i){let r=Ye+i,o=e.length;i>0&&(e[r-1][nn]=n),i<o-Ye?(n[nn]=e[r],oh(e,Ye+i,n)):(e.push(n),n[nn]=null),n[ut]=e;let a=n[Vi];a!==null&&e!==a&&lw(a,n);let s=n[Gn];s!==null&&s.insertView(t),cd(n),n[le]|=128}function lw(t,n){let e=t[$r],i=n[ut];if(hi(i))t[le]|=2;else{let r=i[ut][zt];n[zt]!==r&&(t[le]|=2)}e===null?t[$r]=[n]:e.push(n)}var Wi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ie];return As(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[nt]}set context(n){this._lView[nt]=n}get destroyed(){return Gr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[ut];if(En(n)){let e=n[ys],i=e?e.indexOf(this):-1;i>-1&&(Os(n,i),vs(e,i))}this._attachedToViewContainer=!1}Xd(this._lView[ie],this._lView)}onDestroy(n){wh(this._lView,n)}markForCheck(){sg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[le]&=-129}reattach(){cd(this._lView),this._lView[le]|=128}detectChanges(){this._lView[le]|=1024,tw(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new S(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Lo(this._lView),e=this._lView[Vi];e!==null&&!n&&eg(e,this._lView),H0(this._lView[ie],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new S(902,!1);this._appRef=n;let e=Lo(this._lView),i=this._lView[Vi];i!==null&&!e&&lw(i,this._lView),cd(this._lView)}};var mt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=fk;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=zs(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Wi(o)}}return t})();function fk(){return nu(vt(),ae())}function nu(t,n){return t.type&4?new mt(n,t,qo(t,n)):null}function Yo(t,n,e,i,r){let o=t.data[n];if(o===null)o=mk(t,n,e,i,r),Qb()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=$b();o.injectorIndex=a===null?-1:a.injectorIndex}return Bo(o,!0),o}function mk(t,n,e,i,r){let o=Mh(),a=Th(),s=a?o:o&&o.parent,l=t.data[n]=pk(t,s,e,n,i,r);return hk(t,l,o,a),l}function hk(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function pk(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return Eh()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Fh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function gk(t){let n=t[mh]??[],i=t[ut][Le],r=[];for(let o of n)o.data[g0]!==void 0?r.push(o):_k(o,i);t[mh]=r}function _k(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[p0];for(;e<r;){let o=i.nextSibling;k0(n,i,!1),i=o,e++}}}var vk=()=>null,bk=()=>null;function Nd(t,n){return vk(t,n)}function cw(t,n,e){return bk(t,n,e)}var dw=class{},iu=class{},pp=class{resolveComponentFactory(n){throw new S(917,!1)}},$s=class{static NULL=new pp},bt=class{},je=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>yk()}return t})();function yk(){let t=ae(),n=vt(),e=an(n.index,t);return(hi(e)?e:t)[Le]}var uw=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>null})}return t})();var Id={},gp=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Id,i);return r!==Id||e===Id?r:this.parentInjector.get(n,e,i)}};function Fd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=Zc(r,s);else if(o==2){let l=s,c=n[++a];i=Zc(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function Ue(t,n=0){let e=ae();if(e===null)return V(t,n);let i=vt();return o0(i,e,dt(t),n)}function ru(){let t="invalid";throw new Error(t)}function fw(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let u of a)if(u.resolveHostDirectives!==null){[s,l,c]=u.resolveHostDirectives(a);break}Dk(t,n,e,s,o,l,c)}o!==null&&i!==null&&wk(e,i,o)}function wk(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new S(-301,!1);i.push(n[r],o)}}function Ck(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function Dk(t,n,e,i,r,o,a){let s=i.length,l=null;for(let g=0;g<s;g++){let _=i[g];l===null&&In(_)&&(l=_,Ck(t,e,g)),Xh(Ad(e,n),t,_.type)}Tk(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let g=0;g<s;g++){let _=i[g];_.providersResolver&&_.providersResolver(_)}let c=!1,u=!1,f=L0(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let g=0;g<s;g++){let _=i[g];if(e.mergedAttrs=zo(e.mergedAttrs,_.hostAttrs),Ek(t,e,n,f,_),Mk(f,_,r),a!==null&&a.has(_)){let[O,B]=a.get(_);e.directiveToIndex.set(_.type,[f,O+e.directiveStart,B+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,f);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let C=_.type.prototype;!c&&(C.ngOnChanges||C.ngOnInit||C.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(C.ngOnChanges||C.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}xk(t,e,o)}function xk(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))Ty(0,n,r,i),Ty(1,n,r,i),Ry(n,i,!1);else{let o=e.get(r);ky(0,n,o,i),ky(1,n,o,i),Ry(n,i,!0)}}}function Ty(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),mw(n,o)}}function ky(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),mw(n,a)}}function mw(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Ry(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||qp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){a??=[],a.push(c[u+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function Ek(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Pi(r.type,!0)),a=new Yr(o,In(r),Ue,null);t.blueprint[i]=a,e[i]=a,Ik(t,n,i,L0(t,e,r.hostVars,Qt),r)}function Ik(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;Sk(a)!=s&&a.push(s),a.push(e,i,o)}}function Sk(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function Mk(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;In(n)&&(e[""]=t)}}function Tk(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function lg(t,n,e,i,r,o,a,s){let l=n[ie],c=l.consts,u=Xt(c,a),f=Yo(l,t,e,i,u);return o&&fw(l,n,f,Xt(c,s),r),f.mergedAttrs=zo(f.mergedAttrs,f.attrs),f.attrs!==null&&Fd(f,f.attrs,!1),f.mergedAttrs!==null&&Fd(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function cg(t,n){Ky(t,n),hh(n)&&t.queries.elementEnd(n)}function kk(t,n,e,i,r,o){let a=n.consts,s=Xt(a,r),l=Yo(n,t,e,i,s);if(l.mergedAttrs=zo(l.mergedAttrs,l.attrs),o!=null){let c=Xt(a,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Fd(l,l.attrs,!1),l.mergedAttrs!==null&&Fd(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function Rk(t,n,e){return t[n]=e}function ln(t,n,e){if(e===Qt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Ak(t,n,e,i){let r=ln(t,n,e);return ln(t,n+1,i)||r}function Sd(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&SM(r,o);let a=Wn(t)?an(t.index,n):n;sg(a,5);let s=n[nt],l=Ay(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Ay(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Ay(t,n,e,i){let r=ne(null);try{return ke(xe.OutputStart,n,e),e(i)!==!1}catch(o){return YT(t,o),!1}finally{ke(xe.OutputEnd,n,e),ne(r)}}function hw(t,n,e,i,r,o,a,s){let l=Po(t),c=!1,u=null;if(!i&&l&&(u=Nk(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=a,u.__ngLastListenerFn__=a,c=!0}else{let f=on(t,e),g=i?i(f):f;TM(e,g,o,s),i||(s.__ngNativeEl__=f);let _=r.listen(g,o,s);if(!Ok(o)){let C=i?O=>i(rn(O[t.index])):t.index;pw(C,n,e,o,s,_,!1)}}return c}function Ok(t){return t.startsWith("animation")||t.startsWith("transition")}function Nk(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[No],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function pw(t,n,e,i,r,o,a){let s=n.firstCreatePass?Dh(n):null,l=Ch(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function Oy(t,n,e,i,r,o){let a=n[e],s=n[ie],c=s.data[e].outputs[i],f=a[c].subscribe(o);pw(t.index,s,n,r,o,f,!0)}var _p=Symbol("BINDING");function gw(t){return t.debugInfo?.className||t.type.name||null}var Pd=class extends $s{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=ui(n);return new Zr(e,this.ngModule)}};function Fk(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Yd.SignalBased)!==0};return r&&(o.transform=r),o})}function Pk(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function Lk(t,n,e){let i=n instanceof Fe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new gp(e,i):e}function Bk(t){let n=t.get(bt,null);if(n===null)throw new S(407,!1);let e=t.get(uw,null),i=t.get($n,null),r=t.get(Rn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function jk(t,n){let e=_w(t);return M0(n,e,e==="svg"?ph:e==="math"?Pb:null)}function Vk(t){if(t?.toLowerCase()==="script")throw new S(905,!1)}function _w(t){return(t.selectors[0][0]||"div").toLowerCase()}var Zr=class extends iu{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=Fk(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=Pk(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=pT(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){ke(xe.DynamicComponentStart);let s=ne(null);try{let l=this.componentDef,c=Lk(l,r||this.ngModule,n),u=Bk(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(gw(l),()=>this.createComponentRef(u,c,e,i,o,a)):this.createComponentRef(u,c,e,i,o,a)}finally{ne(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=Hk(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),u=r?jT(c,r,s.encapsulation,e):jk(s,c);Vk(u?.tagName);let f=a?.some(Ny)||o?.some(C=>typeof C!="function"&&C.bindings.some(Ny)),g=Yp(null,l,null,512|P0(s),null,null,n,c,e,null,b0(u,e,!0));g[We]=u,hd(g);let _=null;try{let C=lg(We,g,2,"#host",()=>l.directiveRegistry,!0,0);R0(c,u,C),Uo(u,g),eu(l,g,C),Bp(l,C,g),cg(l,C),i!==void 0&&Uk(C,this.ngContentSelectors,i),_=an(C.index,g),g[nt]=_[nt],ag(l,g,null)}catch(C){throw _!==null&&ep(_),ep(g),C}finally{ke(xe.DynamicComponentEnd),pd()}return new Ld(this.componentType,g,!!f)}};function Hk(t,n,e,i){let r=t?["ng-version","21.2.18"]:gT(n.selectors[0]),o=null,a=null,s=0;if(e)for(let u of e)s+=u[_p].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(a??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let g of f.bindings){s+=g[_p].requiredVars;let _=u+1;g.create&&(g.targetIdx=_,(o??=[]).push(g)),g.update&&(g.targetIdx=_,(a??=[]).push(g))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,g=Jc(f);l.push(g)}return Qp(0,null,zk(o,a),1,s,l,null,null,null,[r],null)}function zk(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Ny(t){let n=t[_p].kind;return n==="input"||n==="twoWay"}var Ld=class extends dw{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=sd(e[ie],We),this.location=qo(this._tNode,e),this.instance=an(this._tNode.index,e)[nt],this.hostView=this.changeDetectorRef=new Wi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=tu(i,r[ie],r,n,e);this.previousInputValues.set(n,e);let a=an(i.index,r);sg(a,1)}get injector(){return new Qr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function Uk(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ht=(()=>{class t{static __NG_ELEMENT_ID__=$k}return t})();function $k(){let t=vt();return vw(t,ae())}var vp=class t extends ht{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return qo(this._hostTNode,this._hostLView)}get injector(){return new Qr(this._hostTNode,this._hostLView)}get parentInjector(){let n=Fp(this._hostTNode,this._hostLView);if(Jy(n)){let e=kd(n,this._hostLView),i=Td(n),r=e[ie].data[i+8];return new Qr(r,e)}else return new Qr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Fy(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Ye}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=Nd(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,$o(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!nM(n),c;if(l)c=e;else{let B=e||{};c=B.index,i=B.injector,r=B.projectableNodes,o=B.environmentInjector||B.ngModuleRef,a=B.directives,s=B.bindings}let u=l?n:new Zr(ui(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let Y=(l?f:this.parentInjector).get(Fe,null);Y&&(o=Y)}let g=ui(u.componentType??{}),_=Nd(this._lContainer,g?.id??null),C=_?.firstChild??null,O=u.create(f,r,C,o,a,s);return this.insertImpl(O.hostView,c,$o(this._hostTNode,_)),O}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Bb(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[ut],c=new t(l,l[Ht],l[ut]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Us(a,r,o,i),n.attachToViewContainerRef(),oh($h(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Fy(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Os(this._lContainer,e);i&&(vs($h(this._lContainer),e),Xd(i[ie],i))}detach(n){let e=this._adjustIndex(n,-1),i=Os(this._lContainer,e);return i&&vs($h(this._lContainer),e)!=null?new Wi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Fy(t){return t[ys]}function $h(t){return t[ys]||(t[ys]=[])}function vw(t,n){let e,i=n[t.index];return En(i)?e=i:(e=ow(i,n,null,t),n[t.index]=e,Kp(n,e)),Wk(e,n,t,i),new vp(e,t,n)}function Gk(t,n){let e=t[Le],i=e.createComment(""),r=on(n,t),o=e.parentNode(r);return Od(e,o,i,e.nextSibling(r),!1),i}var Wk=Yk,qk=()=>!1;function Qk(t,n,e){return qk(t,n,e)}function Yk(t,n,e,i){if(t[zi])return;let r;e.type&8?r=rn(i):r=Gk(n,e),t[zi]=r}var bp=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},yp=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)ug(n,e).matches!==null&&this.queries[e].setDirty()}},Bd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=eR(n):this.predicate=n}},wp=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Cp=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,Kk(e,o)),this.matchTNodeWithReadOption(n,e,Ed(e,n,o,!1,!1))}else i===mt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Ed(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===L||r===ht||r===mt&&e.type&4)this.addMatch(e.index,-2);else{let o=Ed(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function Kk(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function Zk(t,n){return t.type&11?qo(t,n):t.type&4?nu(t,n):null}function Xk(t,n,e,i){return e===-1?Zk(n,t):e===-2?Jk(t,n,i):ks(t,t[ie],e,n)}function Jk(t,n,e){if(e===L)return qo(n,t);if(e===mt)return nu(n,t);if(e===ht)return vw(n,t)}function bw(t,n,e,i){let r=n[Gn].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let u=o[c];s.push(Xk(n,u,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function Dp(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=bw(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],u=n[-l];for(let f=Ye;f<u.length;f++){let g=u[f];g[Vi]===g[ut]&&Dp(g[ie],g,c,i)}if(u[$r]!==null){let f=u[$r];for(let g=0;g<f.length;g++){let _=f[g];Dp(_[ie],_,c,i)}}}}}return i}function dg(t,n){return t[Gn].queries[n].queryList}function yw(t,n,e){let i=new Tn((e&4)===4);return Hb(t,n,i,i.destroy),(n[Gn]??=new yp).queries.push(new bp(i))-1}function ww(t,n,e){let i=ze();return i.firstCreatePass&&(Dw(i,new Bd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),yw(i,ae(),n)}function Cw(t,n,e,i){let r=ze();if(r.firstCreatePass){let o=vt();Dw(r,new Bd(n,e,i),o.index),tR(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return yw(r,ae(),e)}function eR(t){return t.split(",").map(n=>n.trim())}function Dw(t,n,e){t.queries===null&&(t.queries=new wp),t.queries.track(new Cp(n,e))}function tR(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function ug(t,n){return t.queries.getByIndex(n)}function xw(t,n){let e=t[ie],i=ug(e,n);return i.crossesNgTemplate?Dp(e,t,n,[]):bw(e,t,i,n)}function Ew(t,n,e){let i,r=Ja(()=>{i._dirtyCounter();let o=nR(i,t);if(n&&o===void 0)throw new S(-951,!1);return o});return i=r[Xe],i._dirtyCounter=F(0),i._flatValue=void 0,r}function fg(t){return Ew(!0,!1,t)}function mg(t){return Ew(!0,!0,t)}function Iw(t,n){let e=t[Xe];e._lView=ae(),e._queryIndex=n,e._queryList=dg(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function nR(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[le]&4)return n?void 0:Ot;let r=dg(e,i),o=xw(e,i);return r.reset(o,l0),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Kn=class{},ou=class{};var jd=class extends Kn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Pd(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=nh(n);this._bootstrapComponents=O0(o.bootstrap),this._r3Injector=Ph(n,e,[{provide:Kn,useValue:this},{provide:$s,useValue:this.componentFactoryResolver},...i],hs(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Vd=class extends ou{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new jd(this.moduleType,n,[])}};var Ns=class extends Kn{injector;componentFactoryResolver=new Pd(this);instance=null;constructor(n){super();let e=new Pr([...n.providers,{provide:Kn,useValue:this},{provide:$s,useValue:this.componentFactoryResolver}],n.parent||Oo(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Gs(t,n,e=null){return new Ns({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var iR=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=lh(!1,e.type),r=i.length>0?Gs([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t(V(Fe))})}return t})();function E(t){return Ps(()=>{let n=Sw(t),e=J(b({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===Pp.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(iR).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||kn.Emulated,styles:t.styles||Ot,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Qi("NgStandalone"),Mw(e);let i=t.dependencies;return e.directiveDefs=Py(i,rR),e.pipeDefs=Py(i,Db),e.id=sR(e),e})}function rR(t){return ui(t)||Jc(t)}function R(t){return Ps(()=>({type:t.type,bootstrap:t.bootstrap||Ot,declarations:t.declarations||Ot,imports:t.imports||Ot,exports:t.exports||Ot,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function oR(t,n){if(t==null)return Cn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Yd.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function aR(t){if(t==null)return Cn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function H(t){return Ps(()=>{let n=Sw(t);return Mw(n),n})}function au(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Sw(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Cn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Ot,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:oR(t.inputs,n),outputs:aR(t.outputs),debugInfo:null}}function Mw(t){t.features?.forEach(n=>n(t))}function Py(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function sR(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function hg(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=lR,e.hostDirectives=i?t.map(xp):[t]):i?e.hostDirectives.unshift(...t.map(xp)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function lR(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,Tw(a,n,i),r.set(a,[s,n.length-1])}o===0&&In(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function Tw(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)Ly(xp(o),n,e)}else Ly(i,n,e)}function Ly(t,n,e){let i=Jc(t.directive);cR(i.declaredInputs,t.inputs),Tw(i,n,e),e.set(i,t),n.push(i)}function xp(t){return typeof t=="function"?{directive:dt(t),inputs:Cn,outputs:Cn}:{directive:dt(t.directive),inputs:By(t.inputs),outputs:By(t.outputs)}}function By(t){if(t===void 0||t.length===0)return Cn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function cR(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function dR(t){return Object.getPrototypeOf(t.prototype).constructor}function De(t){let n=dR(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,gs)?n[gs]:void 0,a=Object.hasOwn(n,_s)?n[_s]:void 0;if(In(t))r=o??a;else{if(o)throw new S(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=Gh(t.inputs),l.declaredInputs=Gh(t.declaredInputs),l.outputs=Gh(t.outputs);let c=r.hostBindings;c&&pR(t,c);let u=r.viewQuery,f=r.contentQueries;if(u&&mR(t,u),f&&hR(t,f),uR(t,r),Cb(t.outputs,r.outputs),In(r)&&r.data.animation){let g=t.data;g.animation=(g.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===De&&(e=!1)}}n=Object.getPrototypeOf(n)}fR(i)}function uR(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function fR(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=zo(r.hostAttrs,e=zo(e,r.hostAttrs))}}function Gh(t){return t===Cn?{}:t===Ot?[]:t}function mR(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function hR(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function pR(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function kw(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=zo(t.mergedAttrs,t.attrs);let u=t.tView=Qp(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Bo(t,!1);let l=_R(e,n,t,i);gd()&&tg(e,n,l,t),Uo(l,n);let c=ow(l,n,l,t);n[i+We]=c,Kp(n,c),Qk(c,t,n)}function gR(t,n,e,i,r,o,a,s,l,c,u){let f=e+We,g;return n.firstCreatePass?(g=Yo(n,f,4,a||null,s||null),dd()&&fw(n,t,g,Xt(n.consts,c),ig),Ky(n,g)):g=n.data[f],kw(g,t,n,e,i,r,o,l),Po(g)&&eu(n,t,g),c!=null&&Hs(t,g,u),g}function Go(t,n,e,i,r,o,a,s,l,c,u){let f=e+We,g;if(n.firstCreatePass){if(g=Yo(n,f,4,a||null,s||null),c!=null){let _=Xt(n.consts,c);g.localNames=[];for(let C=0;C<_.length;C+=2)g.localNames.push(_[C],-1)}}else g=n.data[f];return kw(g,t,n,e,i,r,o,l),c!=null&&Hs(t,g,u),g}function He(t,n,e,i,r,o,a,s){let l=ae(),c=ze(),u=Xt(c.consts,o);return gR(l,c,t,n,e,i,r,u,void 0,a,s),He}function Ws(t,n,e,i,r,o,a,s){let l=ae(),c=ze(),u=Xt(c.consts,o);return Go(l,c,t,n,e,i,r,u,void 0,a,s),Ws}var _R=vR;function vR(t,n,e,i){return Es(!0),n[Le].createComment("")}var su=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function Yi(t){return typeof t=="function"&&t[Xe]!==void 0}var pg=new y("");function Ki(t){return!!t&&typeof t.then=="function"}function gg(t){return!!t&&typeof t.subscribe=="function"}var lu=new y("");function cu(t){return xt([{provide:lu,multi:!0,useValue:t}])}var _g=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(lu,{optional:!0})??[];injector=d(j);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ft(this.injector,r);if(Ki(o))e.push(o);else if(gg(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),du=new y("");function Rw(){wm(()=>{let t="";throw new S(600,t)})}function Aw(t){return t.isBoundToModule}var bR=10;var Et=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(sn);afterRenderManager=d(Zd);zonelessEnabled=d(Is);rootEffectScheduler=d(vd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new x;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(pi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(N(e=>!e))}constructor(){d(Rn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Fe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=j.NULL){return this._injector.get(G).run(()=>{ke(xe.BootstrapComponentStart);let a=e instanceof iu;if(!this._injector.get(_g).done){let C="";throw new S(405,C)}let l;a?l=e:l=this._injector.get($s).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=Aw(l)?void 0:this._injector.get(Kn),u=i||l.selector,f=l.create(r,[],u,c),g=f.location.nativeElement,_=f.injector.get(pg,null);return _?.registerApplication(g),f.onDestroy(()=>{this.detachView(f.hostView),Ts(this.components,f),_?.unregisterApplication(g)}),this._loadComponent(f),ke(xe.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ke(xe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Kd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ke(xe.ChangeDetectionEnd),new S(101,!1);let e=ne(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ne(e),this.afterTick.next(),ke(xe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(bt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<bR;){ke(xe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ke(xe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ws(r))continue;let o=i&&!this.zonelessEnabled?0:1;tw(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>ws(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Ts(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(du,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Ts(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new S(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ts(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function qs(t,n){let e=ae(),i=Ui();if(ln(e,i,n)){let r=ze(),o=Ds();if(tu(o,r,e,t,n))Wn(o)&&Q0(e,o.index);else{let s=on(o,e);Y0(e[Le],s,null,o.value,t,n,null)}}return qs}function ee(t,n,e,i){let r=ae(),o=Ui();if(ln(r,o,n)){let a=ze(),s=Ds();qT(s,r,t,n,e,i)}return ee}var Ep=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Wh(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function yR(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){ne(i);let c=n.length-1;for(ne(null);a<=s&&a<=c;){let u=t.at(a),f=n[a],g=Wh(a,u,a,f,e);if(g!==0){g<0&&t.updateValue(a,f),a++;continue}let _=t.at(s),C=n[c],O=Wh(s,_,c,C,e);if(O!==0){O<0&&t.updateValue(s,C),s--,c--;continue}let B=e(a,u),Y=e(s,_),Se=e(a,f);if(Object.is(Se,Y)){let gt=e(c,C);Object.is(gt,B)?(t.swap(a,s),t.updateValue(s,C),c--,s--):t.move(s,a),t.updateValue(a,f),a++;continue}if(r??=new Hd,o??=Vy(t,a,s,e),Ip(t,r,a,Se))t.updateValue(a,f),a++,s++;else if(o.has(Se))r.set(B,t.detach(a)),s--;else{let gt=t.create(a,n[a]);t.attach(a,gt),a++,s++}}for(;a<=c;)jy(t,r,e,a,n[a]),a++}else if(n!=null){ne(i);let c=n[Symbol.iterator]();ne(null);let u=c.next();for(;!u.done&&a<=s;){let f=t.at(a),g=u.value,_=Wh(a,f,a,g,e);if(_!==0)_<0&&t.updateValue(a,g),a++,u=c.next();else{r??=new Hd,o??=Vy(t,a,s,e);let C=e(a,g);if(Ip(t,r,a,C))t.updateValue(a,g),a++,s++,u=c.next();else if(!o.has(C))t.attach(a,t.create(a,g)),a++,s++,u=c.next();else{let O=e(a,f);r.set(O,t.detach(a)),s--}}}for(;!u.done;)jy(t,r,e,t.length,u.value),u=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function Ip(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function jy(t,n,e,i,r){if(Ip(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Vy(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Hd=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function T(t,n,e,i,r,o,a,s){Qi("NgControlFlow");let l=ae(),c=ze(),u=Xt(c.consts,o);return Go(l,c,t,n,e,i,r,u,256,a,s),vg}function vg(t,n,e,i,r,o,a,s){Qi("NgControlFlow");let l=ae(),c=ze(),u=Xt(c.consts,o);return Go(l,c,t,n,e,i,r,u,512,a,s),vg}function k(t,n){Qi("NgControlFlow");let e=ae(),i=Ui(),r=e[i]!==Qt?e[i]:-1,o=r!==-1?zd(e,We+r):void 0,a=0;if(ln(e,i,t)){let s=ne(null);try{if(o!==void 0&&sw(o,a),t!==-1){let l=We+t,c=zd(e,l),u=kp(e[ie],l),f=cw(c,u,e),g=zs(e,u,n,{dehydratedView:f});Us(c,g,a,$o(u,f))}}finally{ne(s)}}else if(o!==void 0){let s=aw(o,a);s!==void 0&&(s[nt]=n)}}var Sp=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Ye}};function Qs(t){return t}function dn(t,n){return n}var Mp=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function It(t,n,e,i,r,o,a,s,l,c,u,f,g){Qi("NgControlFlow");let _=ae(),C=ze(),O=l!==void 0,B=ae(),Y=s?a.bind(B[zt][nt]):a,Se=new Mp(O,Y);B[We+t]=Se,Go(_,C,t+1,n,e,i,r,Xt(C.consts,o),256),O&&Go(_,C,t+2,l,c,u,f,Xt(C.consts,g),512)}var Tp=class extends Ep{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Ye}at(n){return this.getLView(n)[nt].$implicit}attach(n,e){let i=e[Vr];this.needsIndexUpdate||=n!==this.length,Us(this.lContainer,e,n,$o(this.templateTNode,i)),wR(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,CR(this.lContainer,n),DR(this.lContainer,n)}create(n,e){let i=Nd(this.lContainer,this.templateTNode.tView.ssrId);return zs(this.hostLView,this.templateTNode,new Sp(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Xd(n[ie],n)}updateValue(n,e){this.getLView(n)[nt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[nt].$index=n}getLView(n){return xR(this.lContainer,n)}};function St(t){let n=ne(null),e=qn();try{let i=ae(),r=i[ie],o=i[e],a=e+1,s=zd(i,a);if(o.liveCollection===void 0){let c=kp(r,a);o.liveCollection=new Tp(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(yR(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Ui(),u=l.length===0;if(ln(i,c,u)){let f=e+2,g=zd(i,f);if(u){let _=kp(r,f),C=cw(g,_,i),O=zs(i,_,void 0,{dehydratedView:C});Us(g,O,0,$o(_,C))}else r.firstUpdatePass&&gk(g),sw(g,0)}}}finally{ne(n)}}function zd(t,n){return t[n]}function wR(t,n){if(t.length<=Ye)return;let e=Ye+n,i=t[e],r=i?i[Hi]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[fi];ET(o,r),Kr.delete(i[mi]),r.detachedLeaveAnimationFns=void 0}}function CR(t,n){if(t.length<=Ye)return;let e=Ye+n,i=t[e],r=i?i[Hi]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function DR(t,n){return Os(t,n)}function xR(t,n){return aw(t,n)}function kp(t,n){return sd(t,n)}function $(t,n,e){let i=ae(),r=Ui();if(ln(i,r,n)){let o=ze(),a=Ds();UT(a,i,t,n,i[Le],e)}return $}function Rp(t,n,e,i,r){tu(n,t,e,r?"class":"style",i)}function h(t,n,e,i){let r=ae(),o=r[ie],a=t+We,s=o.firstCreatePass?lg(a,r,2,n,ig,dd(),e,i):o.data[a];if(Wn(s)){let l=r[xn].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(gw(c),()=>(Hy(t,n,r,s,i),h))}}return Hy(t,n,r,s,i),h}function Hy(t,n,e,i,r){if(rg(i,e,t,n,Ow),Po(i)){let o=e[ie];eu(o,e,i),Bp(o,i,e)}r!=null&&Hs(e,i)}function p(){let t=ze(),n=vt(),e=og(n);return t.firstCreatePass&&cg(t,e),Ih(e)&&Sh(),xh(),e.classesWithoutHost!=null&&lM(e)&&Rp(t,e,ae(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&cM(e)&&Rp(t,e,ae(),e.stylesWithoutHost,!1),p}function re(t,n,e,i){return h(t,n,e,i),p(),re}function rt(t,n,e,i){let r=ae(),o=r[ie],a=t+We,s=o.firstCreatePass?kk(a,o,2,n,e,i):o.data[a];return rg(s,r,t,n,Ow),i!=null&&Hs(r,s),rt}function pt(){let t=vt(),n=og(t);return Ih(n)&&Sh(),xh(),pt}function Yt(t,n,e,i){return rt(t,n,e,i),pt(),Yt}var Ow=(t,n,e,i,r)=>(Es(!0),M0(n[Le],i,Fh()));function yt(t,n,e){let i=ae(),r=i[ie],o=t+We,a=r.firstCreatePass?lg(o,i,8,"ng-container",ig,dd(),n,e):r.data[o];if(rg(a,i,t,"ng-container",ER),Po(a)){let s=i[ie];eu(s,i,a),Bp(s,a,i)}return e!=null&&Hs(i,a),yt}function wt(){let t=ze(),n=vt(),e=og(n);return t.firstCreatePass&&cg(t,e),wt}var ER=(t,n,e,i,r)=>(Es(!0),eT(n[Le],""));function $e(){return ae()}function Mt(t,n,e){let i=ae(),r=Ui();if(ln(i,r,n)){let o=ze(),a=Ds();q0(a,i,t,n,i[Le],e)}return Mt}var Ys="en-US";var IR=Ys;function Nw(t){typeof t=="string"&&(IR=t.toLowerCase().replace(/_/g,"-"))}function W(t,n,e){let i=ae(),r=ze(),o=vt();return SR(r,i,i[Le],o,t,n,e),W}function Ko(t,n,e){let i=ae(),r=ze(),o=vt();return(o.type&3||e)&&hw(o,r,i,e,i[Le],t,n,Sd(o,i,n)),Ko}function SR(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Sd(i,n,o),hw(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let g=u[f],_=u[f+1];l??=Sd(i,n,o),Oy(i,n,g,_,r,l)}if(c&&c.length)for(let f of c)l??=Sd(i,n,o),Oy(i,n,f,r,r,l)}}function D(t=1){return ty(t)}function MR(t,n){let e=null,i=dT(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?F0(t,o,!0):mT(i,o))return r}return e}function me(t){let n=ae()[zt][Ht];if(!n.projection){let e=t?t.length:1,i=n.projection=Tb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?MR(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function z(t,n=0,e,i,r,o){let a=ae(),s=ze(),l=i?t+1:null;l!==null&&Go(a,s,l,i,r,o,null,e);let c=Yo(s,We+t,16,null,e||null);c.projection===null&&(c.projection=n),kh();let f=!a[Vr]||Eh();a[zt][Ht].projection[c.projection]===null&&l!==null?TR(a,s,l):f&&!Wd(c)&&PT(s,a,c)}function TR(t,n,e){let i=We+e,r=n.data[i],o=t[i],a=Nd(o,r.tView.ssrId),s=zs(t,r,void 0,{dehydratedView:a});Us(o,s,0,$o(r,a))}function Ke(t,n,e,i){return Cw(t,n,e,i),Ke}function Re(t,n,e){return ww(t,n,e),Re}function q(t){let n=ae(),e=ze(),i=md();Cs(i+1);let r=ug(e,i);if(t.dirty&&Lb(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=xw(n,i);t.reset(o,l0),t.notifyOnChanges()}return!0}return!1}function Q(){return dg(ae(),md())}function uu(t,n,e,i,r){return Iw(n,Cw(t,e,i,r)),uu}function fu(t,n,e,i){return Iw(t,ww(n,e,i)),fu}function mu(t=1){Cs(md()+t)}function Ct(t){let n=Gb();return _h(n,We+t)}function Cd(t,n){return t<<17|n<<2}function Xr(t){return t>>17&32767}function kR(t){return(t&2)==2}function RR(t,n){return t&131071|n<<17}function Ap(t){return t|2}function Wo(t){return(t&131068)>>2}function qh(t,n){return t&-131069|n<<2}function AR(t){return(t&1)===1}function Op(t){return t|1}function OR(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Xr(a),l=Wo(a);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||Ao(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let g=Xr(t[s+1]);t[i+1]=Cd(g,s),g!==0&&(t[g+1]=qh(t[g+1],i)),t[s+1]=RR(t[s+1],i)}else t[i+1]=Cd(s,0),s!==0&&(t[s+1]=qh(t[s+1],i)),s=i;else t[i+1]=Cd(l,0),s===0?s=i:t[l+1]=qh(t[l+1],i),l=i;c&&(t[i+1]=Ap(t[i+1])),zy(t,u,i,!0),zy(t,u,i,!1),NR(n,u,t,i,o),a=Cd(s,l),o?n.classBindings=a:n.styleBindings=a}function NR(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Ao(o,n)>=0&&(e[i+1]=Op(e[i+1]))}function zy(t,n,e,i){let r=t[e+1],o=n===null,a=i?Xr(r):Wo(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];FR(l,n)&&(s=!0,t[a+1]=i?Op(c):Ap(c)),a=i?Xr(c):Wo(c)}s&&(t[e+1]=i?Ap(r):Op(r))}function FR(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Ao(t,n)>=0:!1}var Mn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function PR(t){return t.substring(Mn.key,Mn.keyEnd)}function LR(t){return BR(t),Fw(t,Pw(t,0,Mn.textEnd))}function Fw(t,n){let e=Mn.textEnd;return e===n?-1:(n=Mn.keyEnd=jR(t,Mn.key=n,e),Pw(t,n,e))}function BR(t){Mn.key=0,Mn.keyEnd=0,Mn.value=0,Mn.valueEnd=0,Mn.textEnd=t.length}function Pw(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function jR(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Tt(t,n,e){return Lw(t,n,e,!1),Tt}function P(t,n){return Lw(t,n,null,!0),P}function st(t){HR(qR,VR,t,!0)}function VR(t,n){for(let e=LR(n);e>=0;e=Fw(n,e))id(t,PR(n),!0)}function Lw(t,n,e,i){let r=ae(),o=ze(),a=ud(2);if(o.firstUpdatePass&&jw(o,t,a,i),n!==Qt&&ln(r,a,n)){let s=o.data[qn()];Vw(o,s,r,r[Le],t,r[a+1]=YR(n,e),i,a)}}function HR(t,n,e,i){let r=ze(),o=ud(2);r.firstUpdatePass&&jw(r,null,o,i);let a=ae();if(e!==Qt&&ln(a,o,e)){let s=r.data[qn()];if(Hw(s,i)&&!Bw(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=Zc(l,e||"")),Rp(r,s,a,e,i)}else QR(r,s,a,a[Le],a[o+1],a[o+1]=WR(t,n,e),i,o)}}function Bw(t,n){return n>=t.expandoStartIndex}function jw(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[qn()],a=Bw(t,e);Hw(o,i)&&n===null&&!a&&(n=!1),n=zR(r,o,n,i),OR(r,o,n,e,a,i)}}function zR(t,n,e,i){let r=Zb(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Qh(null,t,n,e,i),e=Fs(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=Qh(r,t,n,e,i),o===null){let l=UR(t,n,i);l!==void 0&&Array.isArray(l)&&(l=Qh(null,t,n,l[1],i),l=Fs(l,n.attrs,i),$R(t,n,i,l))}else o=GR(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function UR(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Wo(i)!==0)return t[Xr(i)]}function $R(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Xr(r)]=i}function GR(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Fs(i,a,e)}return Fs(i,n.attrs,e)}function Qh(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Fs(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Fs(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),id(t,a,e?!0:n[++o]))}return t===void 0?null:t}function WR(t,n,e){if(e==null||e==="")return Ot;let i=[],r=cn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function qR(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&id(t,i,e)}function QR(t,n,e,i,r,o,a,s){r===Qt&&(r=Ot);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let g=l<r.length?r[l+1]:void 0,_=c<o.length?o[c+1]:void 0,C=null,O;u===f?(l+=2,c+=2,g!==_&&(C=f,O=_)):f===null||u!==null&&u<f?(l+=2,C=u):(c+=2,C=f,O=_),C!==null&&Vw(t,n,e,i,C,O,a,s),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function Vw(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],u=AR(c)?Uy(l,n,e,r,Wo(c),a):void 0;if(!Ud(u)){Ud(o)||kR(c)&&(o=Uy(l,null,e,r,s,a));let f=gh(qn(),e);BT(i,a,f,r,o)}}function Uy(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,g=e[r+1];g===Qt&&(g=f?Ot:void 0);let _=f?rd(g,i):u===i?g:void 0;if(c&&!Ud(_)&&(_=rd(l,i)),Ud(_)&&(s=_,a))return s;let C=t[r+1];r=a?Xr(C):Wo(C)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=rd(l,i))}return s}function Ud(t){return t!==void 0}function YR(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=hs(cn(t)))),t}function Hw(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=ae(),i=ze(),r=t+We,o=i.firstCreatePass?Yo(i,r,1,n,null):i.data[r],a=KR(i,e,o,n);e[r]=a,gd()&&tg(i,e,a,o),Bo(o,!1)}var KR=(t,n,e,i)=>(Es(!0),XM(n[Le],i));function ZR(t,n,e,i=""){return ln(t,Ui(),e)?n+Br(e)+i:Qt}function XR(t,n,e,i,r,o=""){let a=Wb(),s=Ak(t,a,e,r);return ud(2),s?n+Br(e)+i+Br(r)+o:Qt}function U(t){return I("",t),U}function I(t,n,e){let i=ae(),r=ZR(i,t,n,e);return r!==Qt&&zw(i,qn(),r),I}function to(t,n,e,i,r){let o=ae(),a=XR(o,t,n,e,i,r);return a!==Qt&&zw(o,qn(),a),to}function zw(t,n,e){let i=gh(n,t);JM(t[Le],i,e)}function $y(t,n,e){let i=ze();i.firstCreatePass&&Uw(n,i.data,i.blueprint,In(t),e)}function Uw(t,n,e,i,r){if(t=dt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Uw(t[o],n,e,i,r);else{let o=ze(),a=ae(),s=vt(),l=Fr(t)?t:dt(t.provide),c=dh(t),u=s.providerIndexes&1048575,f=s.directiveStart,g=s.providerIndexes>>20;if(Fr(t)||!t.multi){let _=new Yr(c,r,Ue,null),C=Kh(l,n,r?u:u+g,f);C===-1?(Xh(Ad(s,a),o,l),Yh(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)):(e[C]=_,a[C]=_)}else{let _=Kh(l,n,u+g,f),C=Kh(l,n,u,u+g),O=_>=0&&e[_],B=C>=0&&e[C];if(r&&!B||!r&&!O){Xh(Ad(s,a),o,l);let Y=tA(r?eA:JR,e.length,r,i,c,t);!r&&B&&(e[C].providerFactory=Y),Yh(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(Y),a.push(Y)}else{let Y=$w(e[r?C:_],c,!r&&i);Yh(o,t,_>-1?_:C,Y)}!r&&i&&B&&e[C].componentProviders++}}}function Yh(t,n,e,i){let r=Fr(n),o=Ob(n);if(r||o){let l=(o?dt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function $w(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Kh(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function JR(t,n,e,i,r){return Np(this.multi,[])}function eA(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=ks(i,i[ie],this.providerFactory.index,r);a=l.slice(0,s),Np(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Np(o,a);return a}function Np(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function tA(t,n,e,i,r,o){let a=new Yr(t,e,Ue,null);return a.multi=[],a.index=n,a.componentProviders=0,$w(a,r,i&&!e),a}function Ae(t,n){return e=>{e.providersResolver=(i,r)=>$y(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>$y(i,r?r(n):n,!0))}}function Ks(t,n,e){return Gw(ae(),Ah(),t,n,e)}function nA(t,n){let e=t[n];return e===Qt?void 0:e}function Gw(t,n,e,i,r,o){let a=n+e;return ln(t,a,r)?Rk(t,a+1,o?i.call(o,r):i(r)):nA(t,a+1)}function vi(t,n){let e=ze(),i,r=t+We;e.firstCreatePass?(i=iA(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Pi(i.type,!0)),a,s=jt(Ue);try{let l=Rd(!1),c=o();return Rd(l),vh(e,ae(),r,c),c}finally{jt(s)}}function iA(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function bi(t,n,e){let i=t+We,r=ae(),o=_h(r,i);return rA(r,i)?Gw(r,Ah(),n,o.transform,e,o):o.transform(e)}function rA(t,n){return t[ie].data[n].pure}function hu(t,n){return nu(t,n)}var $d=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},bg=(()=>{class t{compileModuleSync(e){return new Vd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=nh(e),o=O0(r.declarations).reduce((a,s)=>{let l=ui(s);return l&&a.push(new Zr(l)),a},[]);return new $d(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ww=(()=>{class t{applicationErrorHandler=d(sn);appRef=d(Et);taskService=d(pi);ngZone=d(G);zonelessEnabled=d(Is);tracing=d(Rn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new be;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(fs):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Vh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?oy:Lh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(fs+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qw(){return[{provide:$n,useExisting:Ww},{provide:G,useClass:ms},{provide:Is,useValue:!0}]}function oA(){return typeof $localize<"u"&&$localize.locale||Ys}var pu=new y("",{factory:()=>d(pu,{optional:!0,skipSelf:!0})||oA()});function Ie(t){return _b(t)}function kt(t,n){return Ja(t,n?.equal)}var aA=t=>t;function yg(t,n){if(typeof t=="function"){let e=Bm(t,aA,n?.equal);return Qw(e,n?.debugName)}else{let e=Bm(t.source,t.computation,t.equal);return Qw(e,t.debugName)}}function Qw(t,n){let e=t[Xe],i=t;return i.set=r=>pb(e,r),i.update=r=>gb(e,r),i.asReadonly=_d.bind(t),i}var eC=Symbol("InputSignalNode#UNSET"),EA=J(b({},es),{transformFn:void 0,applyValueToInputSignal(t,n){Dr(t,n)}});function tC(t,n){let e=Object.create(EA);e.value=t,e.transformFn=n?.transform;function i(){if(Ri(e),e.value===eC){let r=null;throw new S(-950,r)}return e.value}return i[Xe]=e,i}var un=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Ls(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Yw(t,n){return tC(t,n)}function IA(t){return tC(eC,t)}var Zo=(Yw.required=IA,Yw);function Kw(t,n){return fg(n)}function SA(t,n){return mg(n)}var Xs=(Kw.required=SA,Kw);function Zw(t,n){return fg(n)}function MA(t,n){return mg(n)}var nC=(Zw.required=MA,Zw);var Cg=new y(""),TA=new y("");function Zs(t){return!t.moduleRef}function kA(t){let n=Zs(t)?t.r3Injector:t.moduleRef.injector,e=n.get(G);return e.run(()=>{Zs(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(sn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Zs(t)){let o=()=>n.destroy(),a=t.platformInjector.get(Cg);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(Cg);a.add(o),t.moduleRef.onDestroy(()=>{Ts(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return AA(i,e,()=>{let o=n.get(pi),a=o.add(),s=n.get(_g);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(pu,Ys);if(Nw(l||Ys),!n.get(TA,!0))return Zs(t)?n.get(Et):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Zs(t)){let u=n.get(Et);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return RA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var RA;function AA(t,n,e){try{let i=e();return Ki(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var gu=null;function OA(t=[],n){return j.create({name:n,providers:[{provide:bs,useValue:"platform"},{provide:Cg,useValue:new Set([()=>gu=null])},...t]})}function NA(t=[]){if(gu)return gu;let n=OA(t);return gu=n,Rw(),FA(n),n}function FA(t){let n=t.get(Gd,null);ft(t,()=>{n?.forEach(e=>e())})}function xg(){return!1}var PA=1e4;var m9=PA-1e3;var ye=(()=>{class t{static __NG_ELEMENT_ID__=LA}return t})();function LA(t){return BA(vt(),ae(),(t&16)===16)}function BA(t,n,e){if(Wn(t)&&!e){let i=an(t.index,n);return new Wi(i,i)}else if(t.type&175){let i=n[zt];return new Wi(i,n)}return null}function iC(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;ke(xe.BootstrapApplicationStart);try{let o=r?.injector??NA(i),a=[qw(),sy,...e||[]],s=new Ns({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return kA({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ke(xe.BootstrapApplicationEnd)}}function te(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Ut(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var wg=Symbol("NOT_SET"),rC=new Set,jA=J(b({},es),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:wg,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==wg&&!Co(this))return this.signal;try{for(let r of this.cleanup??rC)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=oi(this),i;try{i=this.userFn.apply(null,n)}finally{Ai(this,e)}return(this.value===wg||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Dg=class extends Rs{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(it),a),this.scheduler=r;for(let s of Xp){let l=e[s];if(l===void 0)continue;let c=Object.create(jA);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(Ri(c),c.value),c.signal[Xe]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[s]=c,this.hooks[s]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??rC)e()}finally{Oi(n)}}};function oC(t,n){let e=n?.injector??d(j),i=e.get($n),r=e.get(Zd),o=e.get(Rn,null,{optional:!0});r.impl??=e.get(Jp);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(jo,null,{optional:!0}),l=new Dg(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function vu(t,n){let e=ui(t),i=n.elementInjector||Oo();return new Zr(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var aC=null;function yi(){return aC}function Eg(t){aC??=t}var Js=class{},Xo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(sC),providedIn:"platform"})}return t})();var sC=(()=>{class t extends Xo{_location;_history;_doc=d(K);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return yi().getBaseHref(this._doc)}onPopState(e){let i=yi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=yi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function dC(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function lC(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Xi(t){return t&&t[0]!=="?"?`?${t}`:t}var Jo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(HA),providedIn:"root"})}return t})(),VA=new y(""),HA=(()=>{class t extends Jo{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(K).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return dC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Xi(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Xi(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Xi(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(V(Xo),V(VA,8))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ji=(()=>{class t{_subject=new x;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=$A(lC(cC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Xi(i))}normalize(e){return t.stripTrailingSlash(UA(this._basePath,cC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Xi(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Xi(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Xi;static joinWithSlash=dC;static stripTrailingSlash=lC;static \u0275fac=function(i){return new(i||t)(V(Jo))};static \u0275prov=w({token:t,factory:()=>zA(),providedIn:"root"})}return t})();function zA(){return new Ji(V(Jo))}function UA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function cC(t){return t.replace(/\/index\.html$/,"")}function $A(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Ig=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(j);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(Ue(ht))};static \u0275dir=H({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[qe]})}return t})();function el(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var no=class{};var Sg="browser";function uC(t){return t===Sg}var tl=class{_doc;constructor(n){this._doc=n}manager},bu=(()=>{class t extends tl{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(V(K))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Cu=new y(""),Rg=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof bu));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof bu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new S(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(V(Cu),V(G))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Mg="ng-app-id";function fC(t){for(let n of t)n.remove()}function mC(t,n){let e=n.createElement("style");return e.textContent=t,e}function YA(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Mg}="${n}"],link[${Mg}="${n}"]`);if(r)for(let o of r)o.removeAttribute(Mg),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function kg(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Ag=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,YA(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,mC);i?.forEach(r=>this.addUsage(r,this.external,kg))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(fC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])fC(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,mC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,kg(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(V(K),V(qi),V(eo,8),V(Jr))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Tg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Og=/%COMP%/g;var pC="%COMP%",KA=`_nghost-${pC}`,ZA=`_ngcontent-${pC}`,XA=!0,JA=new y("",{factory:()=>XA});function eO(t){return ZA.replace(Og,t)}function tO(t){return KA.replace(Og,t)}function gC(t,n){return n.map(e=>e.replace(Og,t))}var Ng=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new nl(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof wu?r.applyToHost(e):r instanceof il&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case kn.Emulated:o=new wu(l,c,i,this.appId,u,a,s,f);break;case kn.ShadowDom:return new yu(l,e,i,a,s,this.nonce,f,c);case kn.ExperimentalIsolatedShadowDom:return new yu(l,e,i,a,s,this.nonce,f);default:o=new il(l,c,i,u,a,s,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(V(Rg),V(Ag),V(qi),V(JA),V(K),V(G),V(eo),V(Rn,8))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),nl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Tg[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(hC(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(hC(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new S(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Tg[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Tg[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Yn.DashCase|Yn.Important)?n.style.setProperty(e,i,r&Yn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Yn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=yi().getGlobalEventTarget(this.doc,n),!n))throw new S(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function hC(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var yu=class extends nl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=gC(i.id,c);for(let f of c){let g=document.createElement("style");a&&g.setAttribute("nonce",a),g.textContent=f,this.shadowRoot.appendChild(g)}let u=i.getExternalStyles?.();if(u)for(let f of u){let g=kg(f,r);a&&g.setAttribute("nonce",a),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},il=class extends nl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?gC(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Kr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},wu=class extends il{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=eO(c),this.hostAttr=tO(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Du=class t extends Js{supportsDOMEvents=!0;static makeCurrent(){Eg(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=nO();return e==null?null:iO(e)}resetBaseElement(){rl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return el(document.cookie,n)}},rl=null;function nO(){return rl=rl||document.head.querySelector("base"),rl?rl.getAttribute("href"):null}function iO(t){return new URL(t,document.baseURI).pathname}var rO=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),_C=["alt","control","meta","shift"],oO={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},aO={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},vC=(()=>{class t extends tl{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>yi().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),_C.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=oO[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),_C.forEach(a=>{if(a!==r){let s=aO[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(V(K))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function Fg(t,n,e){let i=b({rootComponent:t},sO(n,e));return iC(i)}function sO(t,n){return{platformRef:n?.platformRef,appProviders:[...fO,...t?.providers??[]],platformProviders:uO}}function lO(){Du.makeCurrent()}function cO(){return new Vt}function dO(){return Lp(document),document}var uO=[{provide:Jr,useValue:Sg},{provide:Gd,useValue:lO,multi:!0},{provide:K,useFactory:dO}];var fO=[{provide:bs,useValue:"root"},{provide:Vt,useFactory:cO},{provide:Cu,useClass:bu,multi:!0},{provide:Cu,useClass:vC,multi:!0},Ng,Ag,Rg,{provide:bt,useExisting:Ng},{provide:no,useClass:rO},[]];var er=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=this.headers.get(e);if(!a)return;a=a.filter(s=>o.indexOf(s)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Eu=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Iu=class{encodeKey(n){return bC(n)}encodeValue(n){return bC(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function mO(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var hO=/%(\d[a-f0-9])/gi,pO={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function bC(t){return encodeURIComponent(t).replace(hO,(n,e)=>pO[e]??n)}function xu(t){return`${t}`}var wi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Iu,n.fromString){if(n.fromObject)throw new S(2805,!1);this.map=mO(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(xu):[xu(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(xu(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(xu(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function gO(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function yC(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function wC(t){return typeof Blob<"u"&&t instanceof Blob}function CC(t){return typeof FormData<"u"&&t instanceof FormData}function _O(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var DC="Content-Type",xC="Accept",IC="text/plain",SC="application/json",vO=`${SC}, ${IC}, */*`,ea=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(gO(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new S(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new er,this.context??=new Eu,!this.params)this.params=new wi,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||yC(this.body)||wC(this.body)||CC(this.body)||_O(this.body)?this.body:this.body instanceof wi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||CC(this.body)?null:wC(this.body)?this.body.type||null:yC(this.body)?null:typeof this.body=="string"?IC:this.body instanceof wi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?SC:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer??this.referrer,g=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,C=n.transferCache??this.transferCache,O=n.timeout??this.timeout,B=n.body!==void 0?n.body:this.body,Y=n.withCredentials??this.withCredentials,Se=n.reportProgress??this.reportProgress,gt=n.headers||this.headers,_t=n.params||this.params,Ya=n.context??this.context;return n.setHeaders!==void 0&&(gt=Object.keys(n.setHeaders).reduce((Ka,br)=>Ka.set(br,n.setHeaders[br]),gt)),n.setParams&&(_t=Object.keys(n.setParams).reduce((Ka,br)=>Ka.set(br,n.setParams[br]),_t)),new t(e,i,B,{params:_t,headers:gt,context:Ya,reportProgress:Se,responseType:r,withCredentials:Y,transferCache:C,keepalive:o,cache:s,priority:a,timeout:O,mode:l,redirect:c,credentials:u,referrer:f,integrity:g,referrerPolicy:_})}},io=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(io||{}),na=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new er,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Su=class t extends na{constructor(n={}){super(n)}type=io.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},ol=class t extends na{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=io.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},ta=class extends na{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},bO=200,yO=204;var wO=new y("");var CO=/^\)\]\}',?\n/;var Lg=(()=>{class t{xhrFactory;tracingService=d(Rn,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new S(-2800,!1);let i=this.xhrFactory;return Z(null).pipe(Ee(()=>new oe(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((B,Y)=>a.setRequestHeader(B,Y.join(","))),e.headers.has(xC)||a.setRequestHeader(xC,vO),!e.headers.has(DC)){let B=e.detectContentTypeHeader();B!==null&&a.setRequestHeader(DC,B)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let B=e.responseType.toLowerCase();a.responseType=B!=="json"?B:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let B=a.statusText||"OK",Y=new er(a.getAllResponseHeaders()),Se=a.responseURL||e.url;return l=new Su({headers:Y,status:a.status,statusText:B,url:Se}),l},u=this.maybePropagateTrace(()=>{let{headers:B,status:Y,statusText:Se,url:gt}=c(),_t=null;Y!==yO&&(_t=typeof a.response>"u"?a.responseText:a.response),Y===0&&(Y=_t?bO:0);let Ya=Y>=200&&Y<300;if(e.responseType==="json"&&typeof _t=="string"){let Ka=_t;_t=_t.replace(CO,"");try{_t=_t!==""?JSON.parse(_t):null}catch(br){_t=Ka,Ya&&(Ya=!1,_t={error:br,text:_t})}}Ya?(o.next(new ol({body:_t,headers:B,status:Y,statusText:Se,url:gt||void 0})),o.complete()):o.error(new ta({error:_t,headers:B,status:Y,statusText:Se,url:gt||void 0}))}),f=this.maybePropagateTrace(B=>{let{url:Y}=c(),Se=new ta({error:B,status:a.status||0,statusText:a.statusText||"Unknown Error",url:Y||void 0});o.error(Se)}),g=f;e.timeout&&(g=this.maybePropagateTrace(B=>{let{url:Y}=c(),Se=new ta({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:Y||void 0});o.error(Se)}));let _=!1,C=this.maybePropagateTrace(B=>{_||(o.next(c()),_=!0);let Y={type:io.DownloadProgress,loaded:B.loaded};B.lengthComputable&&(Y.total=B.total),e.responseType==="text"&&a.responseText&&(Y.partialText=a.responseText),o.next(Y)}),O=this.maybePropagateTrace(B=>{let Y={type:io.UploadProgress,loaded:B.loaded};B.lengthComputable&&(Y.total=B.total),o.next(Y)});return a.addEventListener("load",u),a.addEventListener("error",f),a.addEventListener("timeout",g),a.addEventListener("abort",f),e.reportProgress&&(a.addEventListener("progress",C),s!==null&&a.upload&&a.upload.addEventListener("progress",O)),a.send(s),o.next({type:io.Sent}),()=>{a.removeEventListener("error",f),a.removeEventListener("abort",f),a.removeEventListener("load",u),a.removeEventListener("timeout",g),e.reportProgress&&(a.removeEventListener("progress",C),s!==null&&a.upload&&a.upload.removeEventListener("progress",O)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(V(no))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function MC(t,n){return n(t)}function DO(t,n){return(e,i)=>n.intercept(e,{handle:r=>t(r,i)})}function xO(t,n,e){return(i,r)=>ft(e,()=>n(i,o=>t(o,r)))}var TC=new y(""),Bg=new y("",{factory:()=>[]}),kC=new y(""),jg=new y("",{factory:()=>!0});function EO(){let t=null;return(n,e)=>{t===null&&(t=(d(TC,{optional:!0})??[]).reduceRight(DO,MC));let i=d(qr);if(d(jg)){let o=i.add();return t(n,e).pipe(Fi(o))}else return t(n,e)}}var Vg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(Lg),r},providedIn:"root"})}return t})();var Mu=(()=>{class t{backend;injector;chain=null;pendingTasks=d(qr);contributeToStability=d(jg);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Bg),...this.injector.get(kC,[])]));this.chain=i.reduceRight((r,o)=>xO(r,o,this.injector),MC)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Fi(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(V(Vg),V(Fe))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Hg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(Mu),r},providedIn:"root"})}return t})();function Pg(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var ia=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof ea)o=e;else{let l;r.headers instanceof er?l=r.headers:l=new er(r.headers);let c;r.params&&(r.params instanceof wi?c=r.params:c=new wi({fromObject:r.params})),o=new ea(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=Z(o).pipe(To(l=>this.handler.handle(l)));if(e instanceof ea||r.observe==="events")return a;let s=a.pipe(ue(l=>l instanceof ol));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(N(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new S(2806,!1);return l.body}));case"blob":return s.pipe(N(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new S(2807,!1);return l.body}));case"text":return s.pipe(N(l=>{if(l.body!==null&&typeof l.body!="string")throw new S(2808,!1);return l.body}));default:return s.pipe(N(l=>l.body))}case"response":return s;default:throw new S(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new wi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Pg(r,i))}post(e,i,r={}){return this.request("POST",e,Pg(r,i))}put(e,i,r={}){return this.request("PUT",e,Pg(r,i))}static \u0275fac=function(i){return new(i||t)(V(Hg))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var IO=new y("",{factory:()=>!0}),SO="XSRF-TOKEN",MO=new y("",{factory:()=>SO}),TO="X-XSRF-TOKEN",kO=new y("",{factory:()=>TO}),RO=(()=>{class t{cookieName=d(MO);doc=d(K);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=el(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),RC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(RO),r},providedIn:"root"})}return t})();function AO(t,n){if(!d(IO)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Xo).href,{origin:o}=new URL(r),{origin:a}=new URL(t.url,o);if(o!==a)return n(t)}catch{return n(t)}let e=d(RC).getToken(),i=d(kO);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var zg=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t})(zg||{});function OO(t,n){return{\u0275kind:t,\u0275providers:n}}function Ug(...t){let n=[ia,Mu,{provide:Hg,useExisting:Mu},{provide:Vg,useFactory:()=>d(wO,{optional:!0})??d(Lg)},{provide:Bg,useValue:AO,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return xt(n)}var EC=new y("");function $g(){return OO(zg.LegacyInterceptors,[{provide:EC,useFactory:EO},{provide:Bg,useExisting:EC,multi:!0}])}var AC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(V(K))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var al=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(FO),r},providedIn:"root"})}return t})(),FO=(()=>{class t extends al{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case at.NONE:return i;case at.HTML:return Zn(i,"HTML")?cn(i):Qd(this._doc,String(i)).toString();case at.STYLE:return Zn(i,"Style")?cn(i):i;case at.SCRIPT:if(Zn(i,"Script"))return cn(i);throw new S(5200,!1);case at.URL:return Zn(i,"URL")?cn(i):js(String(i));case at.RESOURCE_URL:if(Zn(i,"ResourceURL"))return cn(i);throw new S(5201,!1);default:throw new S(5202,!1)}}bypassSecurityTrustHtml(e){return jp(e)}bypassSecurityTrustStyle(e){return Vp(e)}bypassSecurityTrustScript(e){return Hp(e)}bypassSecurityTrustUrl(e){return zp(e)}bypassSecurityTrustResourceUrl(e){return Up(e)}static \u0275fac=function(i){return new(i||t)(V(K))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var he="primary",yl=Symbol("RouteTitle"),Yg=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function oo(t){return new Yg(t)}function Gg(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function HC(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return Gg(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!Gg(o,t.slice(0,o.length),s)||!Gg(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function Nu(t){return new Promise((n,e)=>{t.pipe(ci()).subscribe({next:i=>n(i),error:i=>e(i)})})}function PO(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Xn(t[e],n[e]))return!1;return!0}function Xn(t,n){let e=t?Kg(t):void 0,i=n?Kg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!zC(t[r],n[r]))return!1;return!0}function Kg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function zC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function LO(t){return t.length>0?t[t.length-1]:null}function lo(t){return os(t)?t:Ki(t)?Pe(Promise.resolve(t)):Z(t)}function UC(t){return os(t)?Nu(t):Promise.resolve(t)}var BO={exact:WC,subset:qC},$C={exact:jO,subset:VO,ignored:()=>!0},GC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Zg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function NC(t,n,e){return BO[e.paths](t.root,n.root,e.matrixParams)&&$C[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function jO(t,n){return Xn(t,n)}function WC(t,n,e){if(!ro(t.segments,n.segments)||!Ru(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!WC(t.children[i],n.children[i],e))return!1;return!0}function VO(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>zC(t[e],n[e]))}function qC(t,n,e){return QC(t,n,n.segments,e)}function QC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!ro(r,e)||n.hasChildren()||!Ru(r,e,i))}else if(t.segments.length===e.length){if(!ro(t.segments,e)||!Ru(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!qC(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!ro(t.segments,r)||!Ru(t.segments,r,i)||!t.children[he]?!1:QC(t.children[he],n,o,i)}}function Ru(t,n,e){return n.every((i,r)=>$C[e](t[r].parameters,i.parameters))}var en=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Me([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=oo(this.queryParams),this._queryParamMap}toString(){return UO.serialize(this)}},Me=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Au(this)}},tr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=oo(this.parameters),this._parameterMap}toString(){return KC(this)}};function HO(t,n){return ro(t,n)&&t.every((e,i)=>Xn(e.parameters,n[i].parameters))}function ro(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function zO(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===he&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==he&&(e=e.concat(n(r,i)))}),e}var fa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new nr,providedIn:"root"})}return t})(),nr=class{parse(n){let e=new Jg(n);return new en(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${sl(n.root,!0)}`,i=WO(n.queryParams),r=typeof n.fragment=="string"?`#${$O(n.fragment)}`:"";return`${e}${i}${r}`}},UO=new nr;function Au(t){return t.segments.map(n=>KC(n)).join("/")}function sl(t,n){if(!t.hasChildren())return Au(t);if(n){let e=t.children[he]?sl(t.children[he],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==he&&i.push(`${r}:${sl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=zO(t,(i,r)=>r===he?[sl(t.children[he],!1)]:[`${r}:${sl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[he]!=null?`${Au(t)}/${e[0]}`:`${Au(t)}/(${e.join("//")})`}}function YC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Tu(t){return YC(t).replace(/%3B/gi,";")}function $O(t){return encodeURI(t)}function Xg(t){return YC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Ou(t){return decodeURIComponent(t)}function FC(t){return Ou(t.replace(/\+/g,"%20"))}function KC(t){return`${Xg(t.path)}${GO(t.parameters)}`}function GO(t){return Object.entries(t).map(([n,e])=>`;${Xg(n)}=${Xg(e)}`).join("")}function WO(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Tu(e)}=${Tu(r)}`).join("&"):`${Tu(e)}=${Tu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var qO=/^[^\/()?;#]+/;function Wg(t){let n=t.match(qO);return n?n[0]:""}var QO=/^[^\/()?;=#]+/;function YO(t){let n=t.match(QO);return n?n[0]:""}var KO=/^[^=?&#]+/;function ZO(t){let n=t.match(KO);return n?n[0]:""}var XO=/^[^&#]+/;function JO(t){let n=t.match(XO);return n?n[0]:""}var Jg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Me([],{}):new Me([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new S(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[he]=new Me(e,i)),r}parseSegment(){let n=Wg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new S(4009,!1);return this.capture(n),new tr(Ou(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=YO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Wg(this.remaining);r&&(i=r,this.capture(i))}n[Ou(e)]=Ou(i)}parseQueryParam(n){let e=ZO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=JO(this.remaining);a&&(i=a,this.capture(i))}let r=FC(e),o=FC(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Wg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new S(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=he);let s=this.parseChildren(e+1);i[a??he]=Object.keys(s).length===1&&s[he]?s[he]:new Me([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new S(4011,!1)}};function ZC(t){return t.segments.length>0?new Me([],{[he]:t}):t}function XC(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=XC(r);if(i===he&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Me(t.segments,n);return eN(e)}function eN(t){if(t.numberOfChildren===1&&t.children[he]){let n=t.children[he];return new Me(t.segments.concat(n.segments),n.children)}return t}function ir(t){return t instanceof en}function JC(t,n,e=null,i=null,r=new nr){let o=eD(t);return tD(o,n,e,i,r)}function eD(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new Me(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=ZC(i);return n??r}function tD(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return qg(o,o,o,e,i,r);let a=tN(n);if(a.toRoot())return qg(o,o,new Me([],{}),e,i,r);let s=nN(a,o,t),l=s.processChildren?cl(s.segmentGroup,s.index,a.commands):iD(s.segmentGroup,s.index,a.commands);return qg(o,s.segmentGroup,l,e,i,r)}function Fu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function fl(t){return typeof t=="object"&&t!=null&&t.outlets}function PC(t,n,e){t||="\u0275";let i=new en;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function qg(t,n,e,i,r,o){let a={};for(let[c,u]of Object.entries(i??{}))a[c]=Array.isArray(u)?u.map(f=>PC(c,f,o)):PC(c,u,o);let s;t===n?s=e:s=nD(t,n,e);let l=ZC(XC(s));return new en(l,a,r)}function nD(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=nD(o,n,e)}),new Me(t.segments,i)}var Pu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Fu(i[0]))throw new S(4003,!1);let r=i.find(fl);if(r&&r!==LO(i))throw new S(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function tN(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Pu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new Pu(e,n,i)}var oa=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function nN(t,n,e){if(t.isAbsolute)return new oa(n,!0,0);if(!e)return new oa(n,!1,NaN);if(e.parent===null)return new oa(e,!0,0);let i=Fu(t.commands[0])?0:1,r=e.segments.length-1+i;return iN(e,r,t.numberOfDoubleDots)}function iN(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new S(4005,!1);r=i.segments.length}return new oa(i,!1,r-o)}function rN(t){return fl(t[0])?t[0].outlets:{[he]:t}}function iD(t,n,e){if(t??=new Me([],{}),t.segments.length===0&&t.hasChildren())return cl(t,n,e);let i=oN(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Me(t.segments.slice(0,i.pathIndex),{});return o.children[he]=new Me(t.segments.slice(i.pathIndex),t.children),cl(o,0,r)}else return i.match&&r.length===0?new Me(t.segments,{}):i.match&&!t.hasChildren()?e_(t,n,e):i.match?cl(t,0,r):e_(t,n,e)}function cl(t,n,e){if(e.length===0)return new Me(t.segments,{});{let i=rN(e),r={};if(Object.keys(i).some(o=>o!==he)&&t.children[he]&&t.numberOfChildren===1&&t.children[he].segments.length===0){let o=cl(t.children[he],n,e);return new Me(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=iD(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new Me(t.segments,r)}}function oN(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(fl(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!BC(l,c,a))return o;i+=2}else{if(!BC(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function e_(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(fl(o)){let l=aN(o.outlets);return new Me(i,l)}if(r===0&&Fu(e[0])){let l=t.segments[n];i.push(new tr(l.path,LC(e[0]))),r++;continue}let a=fl(o)?o.outlets[he]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&Fu(s)?(i.push(new tr(a,LC(s))),r+=2):(i.push(new tr(a,{})),r++)}return new Me(i,{})}function aN(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=e_(new Me([],{}),0,i))}),n}function LC(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function BC(t,n,e){return t==e.path&&Xn(n,e.parameters)}var dl="imperative",Dt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Dt||{}),tn=class{id;url;constructor(n,e){this.id=n,this.url=e}},ao=class extends tn{type=Dt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Jn=class extends tn{urlAfterRedirects;type=Dt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Nt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Nt||{}),ml=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ml||{}),fn=class extends tn{reason;code;type=Dt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function rD(t){return t instanceof fn&&(t.code===Nt.Redirect||t.code===Nt.SupersededByNewNavigation)}var Di=class extends tn{reason;code;type=Dt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},so=class extends tn{error;target;type=Dt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},hl=class extends tn{urlAfterRedirects;state;type=Dt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Lu=class extends tn{urlAfterRedirects;state;type=Dt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Bu=class extends tn{urlAfterRedirects;state;shouldActivate;type=Dt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},ju=class extends tn{urlAfterRedirects;state;type=Dt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Vu=class extends tn{urlAfterRedirects;state;type=Dt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Hu=class{route;type=Dt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},zu=class{route;type=Dt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Uu=class{snapshot;type=Dt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},$u=class{snapshot;type=Dt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Gu=class{snapshot;type=Dt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Wu=class{snapshot;type=Dt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var sa=class{},pl=class{},la=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function sN(t){return!(t instanceof sa)&&!(t instanceof la)&&!(t instanceof pl)}var qu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ma(this.rootInjector)}},ma=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new qu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(V(Fe))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Qu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=t_(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=t_(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=n_(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return n_(n,this._root).map(e=>e.value)}};function t_(t,n){if(t===n.value)return n;for(let e of n.children){let i=t_(t,e);if(i)return i}return null}function n_(t,n){if(t===n.value)return[n];for(let e of n.children){let i=n_(t,e);if(i.length)return i.unshift(n),i}return[]}var Jt=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ra(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var gl=class extends Qu{snapshot;constructor(n,e){super(n),this.snapshot=e,u_(this,n)}toString(){return this.snapshot.toString()}};function oD(t,n){let e=lN(t,n),i=new Je([new tr("",{})]),r=new Je({}),o=new Je({}),a=new Je({}),s=new Je(""),l=new xi(i,r,a,s,o,he,t,e.root);return l.snapshot=e.root,new gl(new Jt(l,[]),e)}function lN(t,n){let e={},i={},r={},a=new ca([],e,r,"",i,he,t,null,{},n);return new _l("",new Jt(a,[]))}var xi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(N(c=>c[yl]))??Z(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(N(n=>oo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(N(n=>oo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function d_(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:b(b({},n.params),t.params),data:b(b({},n.data),t.data),resolve:b(b(b(b({},t.data),n.data),r?.data),t._resolvedData)}:i={params:b({},t.params),data:b({},t.data),resolve:b(b({},t.data),t._resolvedData??{})},r&&sD(r)&&(i.resolve[yl]=r.title),i}var ca=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[yl]}constructor(n,e,i,r,o,a,s,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=oo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=oo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},_l=class extends Qu{url;constructor(n,e){super(e),this.url=n,u_(this,e)}toString(){return aD(this._root)}};function u_(t,n){n.value._routerState=t,n.children.forEach(e=>u_(t,e))}function aD(t){let n=t.children.length>0?` { ${t.children.map(aD).join(", ")} } `:"";return`${t.value}${n}`}function Qg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Xn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Xn(n.params,e.params)||t.paramsSubject.next(e.params),PO(n.url,e.url)||t.urlSubject.next(e.url),Xn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function i_(t,n){let e=Xn(t.params,n.params)&&HO(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||i_(t.parent,n.parent))}function sD(t){return typeof t.title=="string"||t.title===null}var lD=new y(""),wl=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=he;activateEvents=new X;deactivateEvents=new X;attachEvents=new X;detachEvents=new X;routerOutletData=Zo();parentContexts=d(ma);location=d(ht);changeDetector=d(ye);inputBinder=d(Xu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new S(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new S(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new S(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new S(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new r_(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[qe]})}return t})(),r_=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===xi?this.route:n===ma?this.childContexts:n===lD?this.outletData:this.parent.get(n,e)}},Xu=new y("");var f_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&re(0,"router-outlet")},dependencies:[wl],encapsulation:2})}return t})();function m_(t){let n=t.children&&t.children.map(m_),e=n?J(b({},t),{children:n}):b({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==he&&(e.component=f_),e}function cN(t,n,e){let i=vl(t,n._root,e?e._root:void 0);return new gl(i,n)}function vl(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=dN(t,n,e);return new Jt(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>vl(t,s)),a}}let i=uN(n.value),r=n.children.map(o=>vl(t,o));return new Jt(i,r)}}function dN(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return vl(t,i,r);return vl(t,i)})}function uN(t){return new xi(new Je(t.url),new Je(t.params),new Je(t.queryParams),new Je(t.fragment),new Je(t.data),t.outlet,t.component,t)}var da=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},cD="ngNavigationCancelingError";function Yu(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=ir(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=dD(!1,Nt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function dD(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[cD]=!0,e.cancellationCode=n,e}function fN(t){return uD(t)&&ir(t.url)}function uD(t){return!!t&&t[cD]}var o_=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Qg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ra(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ra(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ra(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=ra(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Wu(o.value.snapshot))}),n.children.length&&this.forwardEvent(new $u(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Qg(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),Qg(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},Ku=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},aa=class{component;route;constructor(n,e){this.component=n,this.route=e}};function mN(t,n,e){let i=t._root,r=n?n._root:null;return ll(i,r,e,[i.value])}function hN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function ha(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Xm(t)?t:n.get(t):i}function ll(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ra(n);return t.children.forEach(a=>{pN(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>ul(s,e.getContext(a),r)),r}function pN(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=gN(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Ku(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?ll(t,n,s?s.children:null,i,r):ll(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new aa(s.outlet.component,a))}else a&&ul(n,s,r),r.canActivateChecks.push(new Ku(i)),o.component?ll(t,null,s?s.children:null,i,r):ll(t,null,e,i,r);return r}function gN(t,n,e){if(typeof e=="function")return ft(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!ro(t.url,n.url);case"pathParamsOrQueryParamsChange":return!ro(t.url,n.url)||!Xn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!i_(t,n)||!Xn(t.queryParams,n.queryParams);default:return!i_(t,n)}}function ul(t,n,e){let i=ra(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?ul(a,n.children.getContext(o),e):ul(a,null,e):ul(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new aa(n.outlet.component,r)):e.canDeactivateChecks.push(new aa(null,r)):e.canDeactivateChecks.push(new aa(null,r))}function Cl(t){return typeof t=="function"}function _N(t){return typeof t=="boolean"}function vN(t){return t&&Cl(t.canLoad)}function bN(t){return t&&Cl(t.canActivate)}function yN(t){return t&&Cl(t.canActivateChild)}function wN(t){return t&&Cl(t.canDeactivate)}function CN(t){return t&&Cl(t.canMatch)}function fD(t){return t instanceof Tr||t?.name==="EmptyError"}var ku=Symbol("INITIAL_VALUE");function ua(){return Ee(t=>kr(t.map(n=>n.pipe(Ce(1),Ge(ku)))).pipe(N(n=>{for(let e of n)if(e!==!0){if(e===ku)return ku;if(e===!1||DN(e))return e}return!0}),ue(n=>n!==ku),Ce(1)))}function DN(t){return ir(t)||t instanceof da}function mD(t){return t.aborted?Z(void 0).pipe(Ce(1)):new oe(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function hD(t){return fe(mD(t))}function xN(t){return Lt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Z(J(b({},n),{guardsResult:!0})):EN(o,e,i).pipe(Lt(a=>a&&_N(a)?IN(e,r,t):Z(a)),N(a=>J(b({},n),{guardsResult:a})))})}function EN(t,n,e){return Pe(t).pipe(Lt(i=>RN(i.component,i.route,e,n)),ci(i=>i!==!0,!0))}function IN(t,n,e){return Pe(n).pipe(To(i=>Ni(MN(i.route.parent,e),SN(i.route,e),kN(t,i.path),TN(t,i.route))),ci(i=>i!==!0,!0))}function SN(t,n){return t!==null&&n&&n(new Gu(t)),Z(!0)}function MN(t,n){return t!==null&&n&&n(new Uu(t)),Z(!0)}function TN(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Z(!0);let i=e.map(r=>bn(()=>{let o=n._environmentInjector,a=ha(r,o),s=bN(a)?a.canActivate(n,t):ft(o,()=>a(n,t));return lo(s).pipe(ci())}));return Z(i).pipe(ua())}function kN(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>hN(o)).filter(o=>o!==null).map(o=>bn(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=ha(s,l),u=yN(c)?c.canActivateChild(e,t):ft(l,()=>c(e,t));return lo(u).pipe(ci())});return Z(a).pipe(ua())}));return Z(r).pipe(ua())}function RN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Z(!0);let o=r.map(a=>{let s=n._environmentInjector,l=ha(a,s),c=wN(l)?l.canDeactivate(t,n,e,i):ft(s,()=>l(t,n,e,i));return lo(c).pipe(ci())});return Z(o).pipe(ua())}function AN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Z(!0);let a=o.map(s=>{let l=ha(s,t),c=vN(l)?l.canLoad(n,e):ft(t,()=>l(n,e)),u=lo(c);return r?u.pipe(hD(r)):u});return Z(a).pipe(ua(),pD(i))}function pD(t){return km(tt(n=>{if(typeof n!="boolean")throw Yu(t,n)}),N(n=>n===!0))}function ON(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return Z(!0);let s=a.map(l=>{let c=ha(l,t),u=CN(c)?c.canMatch(n,e,r):ft(t,()=>c(n,e,r));return lo(u).pipe(hD(o))});return Z(s).pipe(ua(),pD(i))}var Ci=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},bl=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function NN(t){throw new S(4e3,!1)}function FN(t){throw dD(!1,Nt.GuardRejected)}var a_=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[he])throw NN(`${n.redirectTo}`);r=r.children[he]}}async applyRedirectCommands(n,e,i,r,o){let a=await PN(e,r,o);if(a instanceof en)throw new bl(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new bl(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new en(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new Me(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new S(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function PN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Nu(lo(ft(e,()=>i(n))))}function LN(t,n){return t.providers&&!t._injector&&(t._injector=Gs(t.providers,n,`Route: ${t.path}`)),t._injector??n}function An(t){return t.outlet||he}function BN(t,n){let e=t.filter(i=>An(i)===n);return e.push(...t.filter(i=>An(i)!==n)),e}var s_={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function gD(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function jN(t,n,e,i,r,o,a){let s=_D(t,n,e);if(!s.matched)return Z(s);let l=gD(o(s));return i=LN(n,i),ON(i,n,e,r,l,a).pipe(N(c=>c===!0?s:b({},s_)))}function _D(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?b({},s_):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||HC)(e,t,n);if(!r)return b({},s_);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?b(b({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function jC(t,n,e,i,r){return e.length>0&&zN(t,e,i,r)?{segmentGroup:new Me(n,HN(i,new Me(e,t.children))),slicedSegments:[]}:e.length===0&&UN(t,e,i)?{segmentGroup:new Me(t.segments,VN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Me(t.segments,t.children),slicedSegments:e}}function VN(t,n,e,i){let r={};for(let o of e)if(Ju(t,n,o)&&!i[An(o)]){let a=new Me([],{});r[An(o)]=a}return b(b({},i),r)}function HN(t,n){let e={};e[he]=n;for(let i of t)if(i.path===""&&An(i)!==he){let r=new Me([],{});e[An(i)]=r}return e}function zN(t,n,e,i){return e.some(r=>!Ju(t,n,r)||!(An(r)!==he)?!1:!(i!==void 0&&An(r)===i))}function UN(t,n,e){return e.some(i=>Ju(t,n,i))}function Ju(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function $N(t,n,e){return n.length===0&&!t.children[e]}var l_=class{};async function GN(t,n,e,i,r,o,a="emptyOnly",s){return new c_(t,n,e,i,r,a,o,s).recognize()}var WN=31,c_=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new a_(this.urlSerializer,this.urlTree)}noMatchError(n){return new S(4002,`'${n.segmentGroup}'`)}async recognize(){let n=jC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Jt(i,e),o=new _l("",r),a=JC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new ca([],Object.freeze({}),Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),he,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,he,e),rootSnapshot:e}}catch(i){if(i instanceof bl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ci?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof Jt?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],u=BN(e,l),f=await this.processSegmentGroup(n,u,c,l,r);a.push(...f)}let s=vD(a);return qN(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof Ci||fD(c))continue;throw c}if($N(i,r,o))return new l_;throw new Ci(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(An(i)!==a&&(a===he||!Ju(r,o,i)))throw new Ci(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new Ci(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:g}=_D(e,r,o);if(!l)throw new Ci(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>WN&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let C=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,gD(_),n),O=await this.applyRedirects.lineralizeSegments(r,C);return this.processSegment(n,i,e,O.concat(g),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new ca(i,r,Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,YN(e),An(e),e.component??e._loadedComponent??null,e,KN(e),n),s=d_(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=gt=>this.createSnapshot(n,i,gt.consumedSegments,gt.parameters,a),l=await Nu(jN(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Ci(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:g,remainingSegments:_}=l,C=this.createSnapshot(n,i,g,f,a),{segmentGroup:O,slicedSegments:B}=jC(e,g,_,c,o);if(B.length===0&&O.hasChildren()){let gt=await this.processChildren(u,c,O,C);return new Jt(C,gt)}if(c.length===0&&B.length===0)return new Jt(C,[]);let Y=An(i)===o,Se=await this.processSegment(u,c,O,B,Y?he:o,!0,C);return new Jt(C,Se instanceof Jt?[Se]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Nu(AN(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw FN(e)}return{routes:[],injector:n}}};function qN(t){t.sort((n,e)=>n.value.outlet===he?-1:e.value.outlet===he?1:n.value.outlet.localeCompare(e.value.outlet))}function QN(t){let n=t.value.routeConfig;return n&&n.path===""}function vD(t){let n=[],e=new Set;for(let i of t){if(!QN(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=vD(i.children);n.push(new Jt(i.value,r))}return n.filter(i=>!e.has(i))}function YN(t){return t.data||{}}function KN(t){return t.resolve||{}}function ZN(t,n,e,i,r,o,a){return Lt(async s=>{let{state:l,tree:c}=await GN(t,n,e,i,s.extractedUrl,r,o,a);return J(b({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function XN(t){return Lt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Z(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of bD(s))o.add(l);let a=0;return Pe(o).pipe(To(s=>r.has(s)?JN(s,e,t):(s.data=d_(s,s.parent,t).resolve,Z(void 0))),tt(()=>a++),Hc(1),Lt(s=>a===o.size?Z(n):et))})}function bD(t){let n=t.children.map(e=>bD(e)).flat();return[t,...n]}function JN(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!sD(i)&&(r[yl]=i.title),bn(()=>(t.data=d_(t,t.parent,e).resolve,e1(r,t,n).pipe(N(o=>(t._resolvedData=o,t.data=b(b({},t.data),o),null)))))}function e1(t,n,e){let i=Kg(t);if(i.length===0)return Z({});let r={};return Pe(i).pipe(Lt(o=>t1(t[o],n,e).pipe(ci(),tt(a=>{if(a instanceof da)throw Yu(new nr,a);r[o]=a}))),Hc(1),N(()=>r),zn(o=>fD(o)?et:rs(o)))}function t1(t,n,e){let i=n._environmentInjector,r=ha(t,i),o=r.resolve?r.resolve(n,e):ft(i,()=>r(n,e));return lo(o)}function VC(t){return Ee(n=>{let e=t(n);return e?Pe(e).pipe(N(()=>n)):Z(n)})}var h_=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===he);return i}getResolvedTitleForRoute(e){return e.data[yl]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(yD),providedIn:"root"})}return t})(),yD=(()=>{class t extends h_{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(V(AC))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),pa=new y("",{factory:()=>({})}),Dl=new y(""),wD=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(bg);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await UC(ft(e,()=>i.loadComponent())),a=await xD(DD(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await CD(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function CD(t,n,e,i){let r=await UC(ft(e,()=>t.loadChildren())),o=await xD(DD(r)),a;o instanceof ou||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,u;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,u=a,l=s.get(Dl,[],{optional:!0,self:!0}).flat()),{routes:l.map(m_),injector:s,factory:u}}function n1(t){return t&&typeof t=="object"&&"default"in t}function DD(t){return n1(t)?t.default:t}async function xD(t){return t}var ef=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(i1),providedIn:"root"})}return t})(),i1=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ED=new y("");var r1=()=>{},ID=new y(""),SD=(()=>{class t{currentNavigation=F(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=F(null);events=new x;transitionAbortWithErrorSubject=new x;configLoader=d(wD);environmentInjector=d(Fe);destroyRef=d(it);urlSerializer=d(fa);rootContexts=d(ma);location=d(Ji);inputBindingEnabled=d(Xu,{optional:!0})!==null;titleStrategy=d(h_);options=d(pa,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(ef);createViewTransition=d(ED,{optional:!0});navigationErrorHandler=d(ID,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Z(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Hu(r)),i=r=>this.events.next(new zu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Ie(()=>{this.transitions?.next(J(b({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Je(null),this.transitions.pipe(ue(i=>i!==null),Ee(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return Z(i).pipe(Ee(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Nt.SupersededByNewNavigation),et;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?J(b({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new Di(s.id,this.urlSerializer.serialize(s.rawUrl),"",ml.IgnoredSameUrlNavigation)),s.resolve(!1),et;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return Z(s).pipe(Ee(f=>(this.events.next(new ao(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?et:Promise.resolve(f))),ZN(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),tt(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=f.urlAfterRedirects,g)),this.events.next(new pl)}),Ee(f=>Pe(i.routesRecognizeHandler.deferredHandle??Z(void 0)).pipe(N(()=>f))),tt(()=>{let f=new hl(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:f,extractedUrl:g,source:_,restoredState:C,extras:O}=s,B=new ao(f,this.urlSerializer.serialize(g),_,C);this.events.next(B);let Y=oD(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=J(b({},s),{targetSnapshot:Y,urlAfterRedirects:g,extras:J(b({},O),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Se=>(Se.finalUrl=g,Se)),Z(i)}else return this.events.next(new Di(s.id,this.urlSerializer.serialize(s.extractedUrl),"",ml.IgnoredByUrlHandlingStrategy)),s.resolve(!1),et}),N(s=>{let l=new Lu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=J(b({},s),{guards:mN(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),xN(s=>this.events.next(s)),Ee(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw Yu(this.urlSerializer,s.guardsResult);let l=new Bu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return et;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",Nt.GuardRejected),et;if(s.guards.canActivateChecks.length===0)return Z(s);let c=new ju(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return et;let u=!1;return Z(s).pipe(XN(this.paramsInheritanceStrategy),tt({next:()=>{u=!0;let f=new Vu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)},complete:()=>{u||this.cancelNavigationTransition(s,"",Nt.NoDataFromResolver)}}))}),VC(s=>{let l=u=>{let f=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let g=u._environmentInjector;f.push(this.configLoader.loadComponent(g,u.routeConfig).then(_=>{u.component=_}))}for(let g of u.children)f.push(...l(g));return f},c=l(s.targetSnapshot.root);return c.length===0?Z(s):Pe(Promise.all(c).then(()=>s))}),VC(()=>this.afterPreactivation()),Ee(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?Pe(c).pipe(N(()=>i)):Z(i)}),Ce(1),Ee(s=>{let l=cN(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=J(b({},s),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new sa);let c=i.beforeActivateHandler.deferredHandle;return c?Pe(c.then(()=>s)):Z(s)}),tt(s=>{new o_(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=r1,l)),this.lastSuccessfulNavigation.set(Ie(this.currentNavigation)),this.events.next(new Jn(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),fe(mD(o.signal).pipe(ue(()=>!r&&!i.targetRouterState),tt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Nt.Aborted)}))),tt({complete:()=>{r=!0}}),fe(this.transitionAbortWithErrorSubject.pipe(tt(s=>{throw s}))),Fi(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Nt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),zn(s=>{if(r=!0,this.destroyed)return i.resolve(!1),et;if(uD(s))this.events.next(new fn(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),fN(s)?this.events.next(new la(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new so(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=ft(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof da){let{message:u,cancellationCode:f}=Yu(this.urlSerializer,c);this.events.next(new fn(i.id,this.urlSerializer.serialize(i.extractedUrl),u,f)),this.events.next(new la(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return et}))}))}cancelNavigationTransition(e,i,r){let o=new fn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Ie(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function o1(t){return t!==dl}var MD=new y("");var TD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(a1),providedIn:"root"})}return t})(),Zu=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},a1=(()=>{class t extends Zu{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),tf=(()=>{class t{urlSerializer=d(fa);options=d(pa,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Ji);urlHandlingStrategy=d(ef);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new en;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof en?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=oD(null,d(Fe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(s1),providedIn:"root"})}return t})(),s1=(()=>{class t extends tf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof ao?this.updateStateMemento():e instanceof Di?this.commitTransition(i):e instanceof hl?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof sa?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof fn&&!rD(e)?this.restoreHistory(i):e instanceof so?this.restoreHistory(i,!0):e instanceof Jn&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=b(b({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=b(b({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?b({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):b({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function p_(t,n){t.events.pipe(ue(e=>e instanceof Jn||e instanceof fn||e instanceof so||e instanceof Di),N(e=>e instanceof Jn||e instanceof Di?0:(e instanceof fn?e.code===Nt.Redirect||e.code===Nt.SupersededByNewNavigation:!1)?2:1),ue(e=>e!==2),Ce(1)).subscribe(()=>{n()})}var rr=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(su);stateManager=d(tf);options=d(pa,{optional:!0})||{};pendingTasks=d(pi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(SD);urlSerializer=d(fa);location=d(Ji);urlHandlingStrategy=d(ef);injector=d(Fe);_events=new x;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(TD);injectorCleanup=d(MD,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Dl,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Xu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new be;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Ie(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof fn&&i.code!==Nt.Redirect&&i.code!==Nt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Jn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof la){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=b({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||o1(r.source)},a);this.scheduleNavigation(s,dl,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}sN(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),dl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=J(b({},o),{browserUrl:e})),r){let c=b({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(sn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ie(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(m_),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,u=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":u=b(b({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let g=r?r.snapshot:this.routerState.snapshot.root;f=eD(g)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return tD(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=ir(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,dl,null,i)}navigate(e,i={skipLocationChange:!1}){return l1(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(yn(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=b({},GC):i===!1?r=b({},Zg):r=b(b({},Zg),i),ir(e))return NC(this.currentUrlTree,e,r);let o=this.parseUrl(e);return NC(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((f,g)=>{s=f,l=g});let u=this.pendingTasks.add();return p_(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function l1(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new S(4008,!1)}var u1=(()=>{class t{router=d(rr);stateManager=d(tf);fragment=F("");queryParams=F({});path=F("");serializer=d(fa);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Jn&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new en(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),nf=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new un("href"),{optional:!0});reactiveHref=yg(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Ie(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Ie(this._target)}_target=F(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Ie(this._queryParams)}_queryParams=F(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Ie(this._fragment)}_fragment=F(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Ie(this._queryParamsHandling)}_queryParamsHandling=F(void 0);set state(e){this._state.set(e)}get state(){return Ie(this._state)}_state=F(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Ie(this._info)}_info=F(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Ie(this._relativeTo)}_relativeTo=F(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Ie(this._preserveFragment)}_preserveFragment=F(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Ie(this._skipLocationChange)}_skipLocationChange=F(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Ie(this._replaceUrl)}_replaceUrl=F(!1);isAnchorElement;onChanges=new x;applicationErrorHandler=d(sn);options=d(pa,{optional:!0});reactiveRouterState=d(u1);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=F(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(ir(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=kt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:ir(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Ie(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(Ue(rr),Ue(xi),Ls("tabindex"),Ue(je),Ue(L),Ue(Jo))};static \u0275dir=H({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&W("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&ee("href",r.reactiveHref(),Gp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",te],skipLocationChange:[2,"skipLocationChange","skipLocationChange",te],replaceUrl:[2,"replaceUrl","replaceUrl",te],routerLink:"routerLink"},features:[qe]})}return t})();var f1=new y("");function g_(t,...n){return xt([{provide:Dl,multi:!0,useValue:t},[],{provide:xi,useFactory:m1},{provide:du,multi:!0,useFactory:h1},n.map(e=>e.\u0275providers)])}function m1(){return d(rr).routerState.root}function h1(){let t=d(j);return n=>{let e=t.get(Et);if(n!==e.components[0])return;let i=t.get(rr),r=t.get(p1);t.get(g1)===1&&i.initialNavigation(),t.get(_1,null,{optional:!0})?.setUpPreloading(),t.get(f1,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var p1=new y("",{factory:()=>new x}),g1=new y("",{factory:()=>1});var _1=new y("");var __="Service workers are disabled or not supported by this browser",ga=class{serviceWorker;worker;registration;events;constructor(n,e){if(this.serviceWorker=n,!n)this.worker=this.events=this.registration=new oe(i=>i.error(new S(5601,!1)));else{let i=null,r=new x;this.worker=new oe(c=>(i!==null&&c.next(i),r.subscribe(u=>c.next(u))));let o=()=>{let{controller:c}=n;c!==null&&(i=c,r.next(i))};n.addEventListener("controllerchange",o),o(),this.registration=this.worker.pipe(Ee(()=>n.getRegistration().then(c=>{if(!c)throw new S(5601,!1);return c})));let a=new x;this.events=a.asObservable();let s=c=>{let{data:u}=c;u?.type&&a.next(u)};n.addEventListener("message",s),e?.get(Et,null,{optional:!0})?.onDestroy(()=>{n.removeEventListener("controllerchange",o),n.removeEventListener("message",s)})}}postMessage(n,e){return new Promise(i=>{this.worker.pipe(Ce(1)).subscribe(r=>{r.postMessage(b({action:n},e)),i()})})}postMessageWithOperation(n,e,i){let r=this.waitForOperationCompleted(i),o=this.postMessage(n,e);return Promise.all([o,r]).then(([,a])=>a)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(n){let e;return typeof n=="string"?e=i=>i.type===n:e=i=>n.includes(i.type),this.events.pipe(ue(e))}nextEventOfType(n){return this.eventsOfType(n).pipe(Ce(1))}waitForOperationCompleted(n){return new Promise((e,i)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(ue(r=>r.nonce===n),Ce(1),N(r=>{if(r.result!==void 0)return r.result;throw new Error(r.error)})).subscribe({next:e,error:i})})}get isEnabled(){return!!this.serviceWorker}},RD=(()=>{class t{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new x;constructor(e){if(this.sw=e,!e.isEnabled){this.messages=li,this.notificationClicks=li,this.notificationCloses=li,this.pushSubscriptionChanges=li,this.subscription=li;return}this.messages=this.sw.eventsOfType("PUSH").pipe(N(r=>r.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(N(r=>r.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(N(r=>r.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(N(r=>r.data)),this.pushManager=this.sw.registration.pipe(N(r=>r.pushManager));let i=this.pushManager.pipe(Ee(r=>r.getSubscription()));this.subscription=new oe(r=>{let o=i.subscribe(r),a=this.subscriptionChanges.subscribe(r);return()=>{o.unsubscribe(),a.unsubscribe()}})}requestSubscription(e){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(__));let i={userVisibleOnly:!0},r=this.decodeBase64(e.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),o=new Uint8Array(new ArrayBuffer(r.length));for(let a=0;a<r.length;a++)o[a]=r.charCodeAt(a);return i.applicationServerKey=o,new Promise((a,s)=>{this.pushManager.pipe(Ee(l=>l.subscribe(i)),Ce(1)).subscribe({next:l=>{this.subscriptionChanges.next(l),a(l)},error:s})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(__));let e=i=>{if(i===null)throw new S(5602,!1);return i.unsubscribe().then(r=>{if(!r)throw new S(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((i,r)=>{this.subscription.pipe(Ce(1),Ee(e)).subscribe({next:i,error:r})})}decodeBase64(e){return atob(e)}static \u0275fac=function(i){return new(i||t)(V(ga))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),rf=(()=>{class t{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(e){if(this.sw=e,!e.isEnabled){this.versionUpdates=li,this.unrecoverable=li;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(__));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let e=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:e},e).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new S(5601,!1));let e=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:e},e)}static \u0275fac=function(i){return new(i||t)(V(ga))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),AD=new y("");function v1(){let t=d(xl);if(!("serviceWorker"in navigator&&t.enabled!==!1))return;let n=d(AD),e=d(G),i=d(Et);e.runOutsideAngular(()=>{let r=navigator.serviceWorker,o=()=>r.controller?.postMessage({action:"INITIALIZE"});r.addEventListener("controllerchange",o),i.onDestroy(()=>{r.removeEventListener("controllerchange",o)})}),e.runOutsideAngular(()=>{let r,{registrationStrategy:o}=t;if(typeof o=="function")r=new Promise(a=>o().subscribe(()=>a()));else{let[a,...s]=(o||"registerWhenStable:30000").split(":");switch(a){case"registerImmediately":r=Promise.resolve();break;case"registerWithDelay":r=kD(+s[0]||0);break;case"registerWhenStable":r=Promise.race([i.whenStable(),kD(+s[0])]);break;default:throw new S(5600,!1)}}r.then(()=>{i.destroyed||navigator.serviceWorker.register(n,{scope:t.scope,updateViaCache:t.updateViaCache,type:t.type}).catch(a=>console.error(yn(5604,!1)))})})}function kD(t){return new Promise(n=>setTimeout(n,t))}function b1(){let t=d(xl),n=d(j),e=!0;return new ga(e&&t.enabled!==!1?navigator.serviceWorker:void 0,n)}var xl=class{enabled;updateViaCache;type;scope;registrationStrategy};function y1(t,n={}){return xt([RD,rf,{provide:AD,useValue:t},{provide:xl,useValue:n},{provide:ga,useFactory:b1},cu(v1)])}var OD=(()=>{class t{static register(e,i={}){return{ngModule:t,providers:[y1(e,i)]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({providers:[RD,rf]})}return t})();function co(t){return t.buttons===0||t.detail===0}function uo(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var v_;function ND(){if(v_==null){let t=typeof document<"u"?document.head:null;v_=!!(t&&(t.createShadowRoot||t.attachShadow))}return v_}function b_(t){if(ND()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function El(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Rt(t){return t.composedPath?t.composedPath()[0]:t.target}var y_;try{y_=typeof Intl<"u"&&Intl.v8BreakIterator}catch{y_=!1}var we=(()=>{class t{_platformId=d(Jr);isBrowser=this._platformId?uC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||y_)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Il;function FD(){if(Il==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Il=!0}))}finally{Il=Il||!1}return Il}function _a(t){return FD()?t:!!t.capture}function $t(t,n=0){return PD(t)?Number(t):arguments.length===2?n:0}function PD(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function ei(t){return t instanceof L?t.nativeElement:t}var LD=new y("cdk-input-modality-detector-options"),BD={ignoreKeys:[18,17,224,91,16]},jD=650,w_={passive:!0,capture:!0},VD=(()=>{class t{_platform=d(we);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Je(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Rt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<jD||(this._modality.next(co(e)?"keyboard":"mouse"),this._mostRecentTarget=Rt(e))};_onTouchstart=e=>{if(uo(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Rt(e)};constructor(){let e=d(G),i=d(K),r=d(LD,{optional:!0});if(this._options=b(b({},BD),r),this.modalityDetected=this._modality.pipe(ss(1)),this.modalityChanged=this.modalityDetected.pipe(Vc()),this._platform.isBrowser){let o=d(bt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,w_),o.listen(i,"mousedown",this._onMousedown,w_),o.listen(i,"touchstart",this._onTouchstart,w_)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Sl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Sl||{}),HD=new y("cdk-focus-monitor-default-options"),of=_a({passive:!0,capture:!0}),Ft=(()=>{class t{_ngZone=d(G);_platform=d(we);_inputModalityDetector=d(VD);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(K);_stopInputModalityDetector=new x;constructor(){let e=d(HD,{optional:!0});this._detectionMode=e?.detectionMode||Sl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Rt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=ei(e);if(!this._platform.isBrowser||r.nodeType!==1)return Z();let o=b_(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new x,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=ei(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=ei(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Sl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Sl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?jD:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Rt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,of),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,of)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(fe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,of),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,of),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var af=new WeakMap,lt=(()=>{class t{_appRef;_injector=d(j);_environmentInjector=d(Fe);load(e){let i=this._appRef=this._appRef||this._injector.get(Et),r=af.get(i);r||(r={loaders:new Set,refs:[]},af.set(i,r),i.onDestroy(()=>{af.get(i)?.refs.forEach(o=>o.destroy()),af.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(vu(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var va=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),sf;function w1(){if(sf===void 0&&(sf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(sf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return sf}function fo(t){return w1()?.createHTML(t)||t}function zD(t,n,e){let i=e.sanitize(at.HTML,n);t.innerHTML=fo(i||"")}function ba(t){return Array.isArray(t)?t:[t]}var UD=new Set,mo,ya=(()=>{class t{_platform=d(we);_nonce=d(eo,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):D1}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&C1(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function C1(t,n){if(!UD.has(t))try{mo||(mo=document.createElement("style"),n&&mo.setAttribute("nonce",n),mo.setAttribute("type","text/css"),document.head.appendChild(mo)),mo.sheet&&(mo.sheet.insertRule(`@media ${t} {body{ }}`,0),UD.add(t))}catch(e){console.error(e)}}function D1(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var On=(()=>{class t{_mediaMatcher=d(ya);_zone=d(G);_queries=new Map;_destroySubject=new x;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return $D(ba(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=$D(ba(e)).map(a=>this._registerQuery(a).observable),o=kr(r);return o=Ni(o.pipe(Ce(1)),o.pipe(ss(1),Rr(0))),o.pipe(N(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new oe(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(Ge(i),N(({matches:a})=>({query:e,matches:a})),fe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function $D(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var x1=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({providers:[x1]})}return t})();var Ca=(()=>{class t{_platform=d(we);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return I1(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=E1(N1(e));if(i&&(GD(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=GD(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!A1(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return O1(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function E1(t){try{return t.frameElement}catch{return null}}function I1(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function S1(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function M1(t){return k1(t)&&t.type=="hidden"}function T1(t){return R1(t)&&t.hasAttribute("href")}function k1(t){return t.nodeName.toLowerCase()=="input"}function R1(t){return t.nodeName.toLowerCase()=="a"}function QD(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function GD(t){if(!QD(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function A1(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function O1(t){return M1(t)?!1:S1(t)||T1(t)||t.hasAttribute("contenteditable")||QD(t)}function N1(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var lf=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?Be(n,{injector:this._injector}):setTimeout(n)}},Ml=(()=>{class t{_checker=d(Ca);_ngZone=d(G);_document=d(K);_injector=d(j);constructor(){d(lt).load(va)}create(e,i=!1){return new lf(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var YD=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),KD=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),F1=0,Tl=(()=>{class t{_ngZone=d(G);_defaultOptions=d(KD,{optional:!0});_liveElement;_document=d(K);_sanitizer=d(al);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(YD,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:zD(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${F1++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var or=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(or||{}),WD="cdk-high-contrast-black-on-white",qD="cdk-high-contrast-white-on-black",C_="cdk-high-contrast-active",ZD=(()=>{class t{_platform=d(we);_hasCheckedHighContrastMode=!1;_document=d(K);_breakpointSubscription;constructor(){this._breakpointSubscription=d(On).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return or.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return or.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return or.BLACK_ON_WHITE}return or.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(C_,WD,qD),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===or.BLACK_ON_WHITE?e.add(C_,WD):i===or.WHITE_ON_BLACK&&e.add(C_,qD)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),kl=(()=>{class t{constructor(){d(ZD)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[wa]})}return t})();var P1=200,cf=class{_letterKeyStream=new x;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new x;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:P1;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(tt(e=>this._pressedLetters.push(e)),Rr(n),ue(()=>this._pressedLetters.length>0),N(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function ct(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Da=class{_items;_activeItemIndex=F(-1);_activeItem=F(null);_wrap=!1;_typeaheadSubscription=be.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Tn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Yi(n)&&(this._effectRef=Gi(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new x;change=new x;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new cf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||ct(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Yi(this._items)?this._items():this._items instanceof Tn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Fl=class extends Da{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Pl=class extends Da{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var D_={},Ve=class t{_appId=d(qi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),D_.hasOwnProperty(n)||(D_[n]=0),`${n}${e?t._infix+"-":""}${D_[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var ex=" ";function I_(t,n,e){let i=uf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(ex)))}function ff(t,n,e){let i=uf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(ex)):t.removeAttribute(n)}function uf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var tx="cdk-describedby-message",df="cdk-describedby-host",E_=0,nx=(()=>{class t{_platform=d(we);_document=d(K);_messageRegistry=new Map;_messagesContainer=null;_id=`${E_++}`;constructor(){d(lt).load(va),this._id=d(qi)+"-"+E_++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=x_(i,r);typeof i!="string"?(JD(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=x_(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${df}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(df);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");JD(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(x_(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=uf(e,"aria-describedby").filter(r=>r.indexOf(tx)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);I_(e,"aria-describedby",r.messageElement.id),e.setAttribute(df,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,ff(e,"aria-describedby",r.messageElement.id),e.removeAttribute(df)}_isElementDescribedByMessage(e,i){let r=uf(e,"aria-describedby"),o=this._messageRegistry.get(i),a=o&&o.messageElement.id;return!!a&&r.indexOf(a)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function x_(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function JD(t,n){t.id||(t.id=`${tx}-${n}-${E_++}`)}var ar={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};function S_(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function ot(t){return t==null?"":typeof t=="string"?t:`${t}px`}var L1=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(K)}),B1=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function ix(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?B1.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Ze=(()=>{class t{get value(){return this.valueSignal()}valueSignal=F("ltr");change=new X;constructor(){let e=d(L1,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(ix(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Nn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Nn||{}),mf,ho;function hf(){if(ho==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return ho=!1,ho;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)ho=!0;else{let t=Element.prototype.scrollTo;t?ho=!/\{\s*\[native code\]\s*\}/.test(t.toString()):ho=!1}}return ho}function xa(){if(typeof document!="object"||!document)return Nn.NORMAL;if(mf==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),mf=Nn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,mf=t.scrollLeft===0?Nn.NEGATED:Nn.INVERTED),t.remove()}return mf}var se=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})();var j1=20,ti=(()=>{class t{_ngZone=d(G);_platform=d(we);_renderer=d(bt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new x;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=j1){return this._platform.isBrowser?new oe(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Bc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Z()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ue(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=ei(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ei=(()=>{class t{elementRef=d(L);scrollDispatcher=d(ti);ngZone=d(G);dir=d(Ze,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new x;_renderer=d(je);_cleanupScroll;_elementScrolled=new x;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&xa()!=Nn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),xa()==Nn.INVERTED?e.left=e.right:xa()==Nn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;hf()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&xa()==Nn.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&xa()==Nn.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),V1=20,hn=(()=>{class t{_platform=d(we);_listeners;_viewportSize=null;_change=new x;_document=d(K);constructor(){let e=d(G),i=d(bt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=V1){return e>0?this._change.pipe(Bc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var mn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})(),Ll=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se,mn,se,mn]})}return t})();var Bl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Fn=class extends Bl{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Pn=class extends Bl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},M_=class extends Bl{element;constructor(n){super(),this.element=n instanceof L?n.nativeElement:n}},sr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Fn)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Pn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof M_)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},jl=class extends sr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Kn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||j.NULL,o=r.get(Fe,i.injector);e=vu(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var lr=(()=>{class t extends sr{_moduleRef=d(Kn,{optional:!0});_document=d(K);_viewContainerRef=d(ht);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new X;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[De]})}return t})(),cr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})();var rx=hf();function Sa(t){return new pf(t.get(hn),t.get(K))}var pf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=ot(-this._previousScrollPosition.left),n.style.top=ot(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),rx&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),rx&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function ux(t,n){return new gf(t.get(ti),t.get(G),t.get(hn),n)}var gf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ue(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Vl=class{enable(){}disable(){}attach(){}};function T_(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function ox(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function Ii(t,n){return new _f(t.get(ti),t.get(hn),t.get(G),n)}var _f=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();T_(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},fx=(()=>{class t{_injector=d(j);constructor(){}noop=()=>new Vl;close=e=>ux(this._injector,e);block=()=>Sa(this._injector);reposition=e=>Ii(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ln=class{positionStrategy;scrollStrategy=new Vl;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var vf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var mx=(()=>{class t{_attachedOverlays=[];_document=d(K);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),hx=(()=>{class t extends mx{_ngZone=d(G);_renderer=d(bt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),px=(()=>{class t extends mx{_platform=d(we);_ngZone=d(G);_renderer=d(bt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Rt(e)};_clickListener=e=>{let i=Rt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(ax(s.overlayElement,i)||ax(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ax(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var gx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),wf=(()=>{class t{_platform=d(we);_containerElement;_document=d(K);_styleLoader=d(lt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||S_()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),S_()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(gx)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),k_=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function R_(t){return t&&t.nodeType===1}var Ea=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new x;_attachments=new x;_detachments=new x;_positionStrategy;_scrollStrategy;_locationChanges=be.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new x;_outsidePointerEvents=new x;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,u=!1,f,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=g,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Be(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=b(b({},this._config),n),this._updateElementSize()}setDirection(n){this._config=J(b({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=ot(this._config.width),n.height=ot(this._config.height),n.minWidth=ot(this._config.minWidth),n.minHeight=ot(this._config.minHeight),n.maxWidth=ot(this._config.maxWidth),n.maxHeight=ot(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;R_(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new k_(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=ba(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Be(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},sx="cdk-overlay-connected-position-bounding-box",z1=/([A-Za-z%]+)$/;function go(t,n){return new bf(n,t.get(hn),t.get(K),t.get(we),t.get(wf))}var bf=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new x;_resizeSubscription=be.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(sx),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),u=this._getOverlayFit(c,e,i,s);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<u.visibleArea)&&(a={overlayFit:u,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&po(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(sx),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof L?this._origin.nativeElement:R_(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=cx(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let u=0-a,f=a+o.width-i.width,g=0-s,_=s+o.height-i.height,C=this._subtractOverflows(o.width,u,f),O=this._subtractOverflows(o.height,g,_),B=C*O;return{visibleArea:B,isCompletelyWithinViewport:o.width*o.height===B,fitsInViewportVertically:O===o.height,fitsInViewportHorizontally:C==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=lx(this._overlayRef.getConfig().minHeight),s=lx(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=cx(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-a:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-s:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!U1(this._lastScrollVisibility,i)){let r=new vf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),C=this._lastBoundingBoxSize.height;o=_*2,a=n.y-_,o>C&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-C/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,g;if(c)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),C=this._lastBoundingBoxSize.width;u=_*2,f=n.x-_,u>C&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-C/2)}return{top:a,left:f,bottom:s,right:g,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=ot(i.width),r.height=ot(i.height),r.top=ot(i.top)||"auto",r.bottom=ot(i.bottom)||"auto",r.left=ot(i.left)||"auto",r.right=ot(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=ot(o)),a&&(r.maxWidth=ot(a))}this._lastBoundingBoxSize=i,po(this._boundingBox.style,r)}_resetBoundingBoxStyles(){po(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){po(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();po(i,this._getExactOverlayY(e,n,u)),po(i,this._getExactOverlayX(e,n,u))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=ot(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=ot(a.maxWidth):o&&(i.maxWidth="")),po(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=ot(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=ot(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:ox(n,i),isOriginOutsideView:T_(n,i),isOverlayClipped:ox(e,i),isOverlayOutsideView:T_(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ba(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof L)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function po(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function lx(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(z1);return!e||e==="px"?parseFloat(n):null}return t||null}function cx(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function U1(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var dx="cdk-global-overlay-wrapper";function dr(t){return new yf}var yf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(dx),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),u=this._xPosition,f=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",_="",C="",O="";l?O="flex-start":u==="center"?(O="center",g?C=f:_=f):g?u==="left"||u==="end"?(O="flex-end",_=f):(u==="right"||u==="start")&&(O="flex-start",C=f):u==="left"||u==="start"?(O="flex-start",_=f):(u==="right"||u==="end")&&(O="flex-end",C=f),n.position=this._cssPosition,n.marginLeft=l?"0":_,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":C,e.justifyContent=O,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(dx),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},_x=(()=>{class t{_injector=d(j);constructor(){}global(){return dr()}flexibleConnectedTo(e){return go(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Hl=new y("OVERLAY_DEFAULT_CONFIG");function jn(t,n){t.get(lt).load(gx);let e=t.get(wf),i=t.get(K),r=t.get(Ve),o=t.get(Et),a=t.get(Ze),s=t.get(je,null,{optional:!0})||t.get(bt).createRenderer(null,null),l=new Ln(n),c=t.get(Hl,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let g=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return R_(g)?g.after(f):g?.type==="parent"?g.element.appendChild(f):e.getContainerElement().appendChild(f),new Ea(new jl(u,o,t),f,u,l,t.get(G),t.get(hx),i,t.get(Ji),t.get(px),n?.disableAnimations??t.get(Bs,null,{optional:!0})==="NoopAnimations",t.get(Fe),s)}var vx=(()=>{class t{scrollStrategies=d(fx);_positionBuilder=d(_x);_injector=d(j);constructor(){}create(e){return jn(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],G1=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Ii(t)}}),Ia=(()=>{class t{elementRef=d(L);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),bx=new y("cdk-connected-overlay-default-config"),Cf=(()=>{class t{_dir=d(Ze,{optional:!0});_injector=d(j);_overlayRef;_templatePortal;_backdropSubscription=be.EMPTY;_attachSubscription=be.EMPTY;_detachSubscription=be.EMPTY;_positionSubscription=be.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(G1);_ngZone=d(G);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new X;positionChange=new X;attach=new X;detach=new X;overlayKeydown=new X;overlayOutsideClick=new X;constructor(){let e=d(mt),i=d(ht),r=d(bx,{optional:!0}),o=d(Hl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Pn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=$1);let e=this._overlayRef=jn(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!ct(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Rt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Ln({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=go(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ia?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ia?this.origin.elementRef.nativeElement:this.origin instanceof L?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Pm(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",te],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",te],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",te],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",te],push:[2,"cdkConnectedOverlayPush","push",te],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",te],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",te],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[qe]})}return t})(),pn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({providers:[vx],imports:[se,cr,Ll,Ll]})}return t})();var W1=new y("MATERIAL_ANIMATIONS"),yx=null;function q1(){return d(W1,{optional:!0})?.animationsDisabled||d(Bs,{optional:!0})==="NoopAnimations"?"di-disabled":(yx??=d(ya).matchMedia("(prefers-reduced-motion)").matches,yx?"reduced-motion":"enabled")}function Oe(){return q1()!=="enabled"}function Gt(t){return t!=null&&`${t}`!="false"}var gn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(gn||{}),A_=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=gn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},wx=_a({passive:!0,capture:!0}),O_=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,wx)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,wx)))}_delegateEventHandler=n=>{let e=Rt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},zl={enterDuration:225,exitDuration:150},Q1=800,Cx=_a({passive:!0,capture:!0}),Dx=["mousedown","touchstart"],xx=["mouseup","mouseleave","touchend","touchcancel"],Y1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),Ul=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new O_;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=ei(i)),o&&o.get(lt).load(Y1)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=b(b({},zl),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||K1(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${s-a}px`,u.style.top=`${l-a}px`,u.style.height=`${a*2}px`,u.style.width=`${a*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),g=f.transitionProperty,_=f.transitionDuration,C=g==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,O=new A_(this,u,i,C);u.style.transform="scale3d(1, 1, 1)",O.state=gn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=O);let B=null;return!C&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let Y=()=>{B&&(B.fallbackTimer=null),clearTimeout(gt),this._finishRippleTransition(O)},Se=()=>this._destroyRipple(O),gt=setTimeout(Se,c+100);u.addEventListener("transitionend",Y),u.addEventListener("transitioncancel",Se),B={onTransitionEnd:Y,onTransitionCancel:Se,fallbackTimer:gt}}),this._activeRipples.set(O,B),(C||!c)&&this._finishRippleTransition(O),O}fadeOutRipple(n){if(n.state===gn.FADING_OUT||n.state===gn.HIDDEN)return;let e=n.element,i=b(b({},zl),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=gn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=ei(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,Dx.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{xx.forEach(e=>{this._triggerElement.addEventListener(e,this,Cx)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===gn.FADING_IN?this._startFadeOutTransition(n):n.state===gn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=gn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=gn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=co(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+Q1;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!uo(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===gn.VISIBLE||n.config.terminateOnPointerUp&&n.state===gn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(Dx.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(xx.forEach(e=>n.removeEventListener(e,this,Cx)),this._pointerUpEventsRegistered=!1))}};function K1(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var N_=new y("mat-ripple-global-options"),ur=(()=>{class t{_elementRef=d(L);_animationsDisabled=Oe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(G),i=d(we),r=d(N_,{optional:!0}),o=d(j);this._globalOptions=r||{},this._rippleRenderer=new Ul(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:b(b(b({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,b(b({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,b(b({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var Z1={capture:!0},X1=["focus","mousedown","mouseenter","touchstart"],F_="mat-ripple-loader-uninitialized",P_="mat-ripple-loader-class-name",Ex="mat-ripple-loader-centered",Df="mat-ripple-loader-disabled",Ix=(()=>{class t{_document=d(K);_animationsDisabled=Oe();_globalRippleOptions=d(N_,{optional:!0});_platform=d(we);_ngZone=d(G);_injector=d(j);_eventCleanups;_hosts=new Map;constructor(){let e=d(bt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>X1.map(i=>e.listen(this._document,i,this._onInteraction,Z1)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(F_,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(P_))&&e.setAttribute(P_,i.className||""),i.centered&&e.setAttribute(Ex,""),i.disabled&&e.setAttribute(Df,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Df,""):e.removeAttribute(Df)}_onInteraction=e=>{let i=Rt(e);if(i instanceof HTMLElement){let r=i.closest(`[${F_}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(P_)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??zl.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??zl.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Df),rippleConfig:{centered:e.hasAttribute(Ex),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Ul(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(F_)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ni=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var J1=["mat-icon-button",""],eF=["*"],tF=new y("MAT_BUTTON_CONFIG");function Sx(t){return t==null?void 0:Ut(t)}var L_=(()=>{class t{_elementRef=d(L);_ngZone=d(G);_animationsDisabled=Oe();_config=d(tF,{optional:!0});_focusMonitor=d(Ft);_cleanupClick;_renderer=d(je);_rippleLoader=d(Ix);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(lt).load(ni);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(ee("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),st(r.color?"mat-"+r.color:""),P("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",te],disabled:[2,"disabled","disabled",te],ariaDisabled:[2,"aria-disabled","ariaDisabled",te],disabledInteractive:[2,"disabledInteractive","disabledInteractive",te],tabIndex:[2,"tabIndex","tabIndex",Sx],_tabindex:[2,"tabindex","_tabindex",Sx]}})}return t})(),_o=(()=>{class t extends L_{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[De],attrs:J1,ngContentSelectors:eF,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(me(),Yt(0,"span",0),z(1),Yt(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var ii=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var nF=["matButton",""],iF=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],rF=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var Mx=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),fr=(()=>{class t extends L_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=oF(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Mx.get(this._appearance):null,o=Mx.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[De],attrs:nF,ngContentSelectors:rF,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(me(iF),Yt(0,"span",0),z(1),rt(2,"span",1),z(3,1),pt(),z(4,2),Yt(5,"span",2)(6,"span",3)),i&2&&P("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function oF(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var vo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[ii,se]})}return t})();function aF(t,n){if(t&1){let e=$e();h(0,"div",1)(1,"button",2),W("click",function(){_e(e);let r=D();return ve(r.action())}),v(2),p()()}if(t&2){let e=D();m(2),I(" ",e.data.action," ")}}var sF=["label"];function lF(t,n){}var cF=Math.pow(2,31)-1,Gl=class{_overlayRef;instance;containerInstance;_afterDismissed=new x;_afterOpened=new x;_onAction=new x;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,cF))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Tx=new y("MatSnackBarData"),Ma=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},dF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),uF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),fF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),mF=(()=>{class t{snackBarRef=d(Gl);data=d(Tx);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(h(0,"div",0),v(1),p(),T(2,aF,3,1,"div",1)),i&2&&(m(),I(" ",r.data.message,`
`),m(),k(r.hasAction?2:-1))},dependencies:[fr,dF,uF,fF],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),B_="_mat-snack-bar-enter",j_="_mat-snack-bar-exit",hF=(()=>{class t extends sr{_ngZone=d(G);_elementRef=d(L);_changeDetectorRef=d(ye);_platform=d(we);_animationsDisabled=Oe();snackBarConfig=d(Ma);_document=d(K);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(j);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new x;_onExit=new x;_onEnter=new x;_animationState="void";_live;_label;_role;_liveElementId=d(Ve).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===j_?this._completeExit():e===B_&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Be(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(B_)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(B_)},200)))}exit(){return this._destroyed?Z(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Be(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(j_)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(j_),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Re(lr,7)(sF,7),i&2){let o;q(o=Q())&&(r._portalOutlet=o.first),q(o=Q())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&W("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&P("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[De],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(h(0,"div",1)(1,"div",2,0)(3,"div",3),He(4,lF,0,0,"ng-template",4),p(),re(5,"div"),p()()),i&2&&(m(5),ee("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[lr],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),pF=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Ma}),kx=(()=>{class t{_live=d(Tl);_injector=d(j);_breakpointObserver=d(On);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(pF);_animationsDisabled=Oe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=mF;snackBarContainerComponent=hF;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=b(b({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=j.create({parent:r||this._injector,providers:[{provide:Ma,useValue:i}]}),a=new Fn(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=b(b(b({},new Ma),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new Gl(a,o);if(e instanceof mt){let l=new Pn(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new Fn(e,void 0,l),u=a.attachComponentPortal(c);s.instance=u.instance}return this._breakpointObserver.observe(ar.HandsetPortrait).pipe(fe(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Ln;i.direction=e.direction;let r=dr(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,jn(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return j.create({parent:r||this._injector,providers:[{provide:Gl,useValue:i},{provide:Tx,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function V_(t){t||(t=d(it));let n=new oe(e=>{if(t.destroyed){e.next();return}return t.onDestroy(e.next.bind(e))});return e=>e.pipe(fe(n))}function Si(t,n){let i=!n?.manualCleanup?n?.injector?.get(it)??d(it):null,r=gF(n?.equal),o;n?.requireSync?o=F({kind:0},{equal:r}):o=F({kind:1,value:n?.initialValue},{equal:r});let a,s=t.subscribe({next:l=>o.set({kind:1,value:l}),error:l=>{o.set({kind:2,error:l}),a?.()},complete:()=>{a?.()}});if(n?.requireSync&&o().kind===0)throw new S(601,!1);return a=i?.onDestroy(s.unsubscribe.bind(s)),kt(()=>{let l=o();switch(l.kind){case 1:return l.value;case 2:throw l.error;case 0:throw new S(601,!1)}},{equal:n?.equal})}function gF(t=Object.is){return(n,e)=>n.kind===1&&e.kind===1&&t(n.value,e.value)}var U_=class{translations;constructor(n){this.translations=n}getTranslation(n){return Z(this.translations.get(n)||{})}},Ox=new y("");function H_(t,n){return t&&(Object.prototype.hasOwnProperty.call(t,n)?t[n]:n.split(".").reduce((e,i)=>e?.[i],t))}function _F(t,n,e){t=b({},t);let i=n.split("."),r=i.length-1;return i.reduce((o,a,s)=>(s===r?o[a]=e:o[a]=Array.isArray(o[a])?o[a].slice():b({},o[a]),o&&o[a]),t),t}function Nx(t){return t?Array.isArray(t)?t.length:If(t)?Object.keys(t).length:t?t.length:0:0}function vF(t){return Nx(t)===0}function bF(t){return typeof t=="function"}function ka(t){return typeof t=="string"}function If(t){return!!t&&typeof t=="object"&&!Array.isArray(t)}function Fx(t){return t.replace(/(?:^\w|[A-Z]|\b\w)/g,(n,e)=>e==0?n.toLowerCase():n.toUpperCase()).replace(/\s+|_|-|\//g,"")}function Px(){return typeof window<"u"}function $_(t){return t==null}function Rx(t){return $_(t)===!1}function Lx(t){return t&&typeof t.scope=="string"}function yF(t){return t&&If(t.loader)}function Ax(t){let n={};function e(i,r){if(i===null)n[r]=null;else if(If(i))for(let[o,a]of Object.entries(i))e(a,r?`${r}.${o}`:o);else n[r]=i}return e(t,""),n}function wF(t){let n={};for(let[e,i]of Object.entries(t)){let r=e.split("."),o=n;r.forEach((a,s)=>{s===r.length-1?o[a]=i:(o[a]??={},o=o[a])})}return n}var Ra=new y("",{providedIn:"root",factory:()=>Ta}),Ta={defaultLang:"en",reRenderOnLangChange:!1,prodMode:!1,failedRetries:2,fallbackLang:[],availableLangs:[],missingHandler:{logMissingKey:!0,useFallbackTranslation:!1,allowEmpty:!1},flatten:{aot:!1},interpolation:["{{","}}"],scopes:{keepCasing:!1}};function CF(t={}){return J(b(b({},Ta),t),{missingHandler:b(b({},Ta.missingHandler),t.missingHandler),flatten:b(b({},Ta.flatten),t.flatten),scopes:b(b({},Ta.scopes),t.scopes)})}var Bx=new y(""),DF=(()=>{class t{config=d(Ra,{optional:!0})??Ta;get interpolationMatcher(){return xF(this.config)}transpile({value:e,params:i={},translation:r,key:o}){if(ka(e)){let a,s=e;for(;(a=this.interpolationMatcher.exec(s))!==null;){let[l,c]=a;s=s.replace(l,()=>{let u=c.trim(),f=H_(i,u);return Rx(f)?f:Rx(r[u])?this.transpile({params:i,translation:r,key:o,value:r[u]}):""})}return s}else i&&(If(e)?e=this.handleObject({value:e,params:i,translation:r,key:o}):Array.isArray(e)&&(e=this.handleArray({value:e,params:i,translation:r,key:o})));return e}handleObject({value:e,params:i={},translation:r,key:o}){let a=e;return Object.keys(i).forEach(s=>{let l=this.transpile({value:H_(a,s),params:H_(i,s),translation:r,key:o});a=_F(a,s,l)}),a}handleArray(r){var o=r,{value:e}=o,i=gm(o,["value"]);return e.map(a=>this.transpile(b({value:a},i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function xF(t){let[n,e]=t.interpolation;return new RegExp(`${n}([^${n}${e}]*?)${e}`,"g")}var jx=new y(""),EF=(()=>{class t{handle(e,i){if(i.missingHandler.logMissingKey&&!i.prodMode){let r=`Missing translation for '${e}'`;console.warn(`%c ${r}`,"font-size: 12px; color: red")}return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Vx=new y(""),IF=(()=>{class t{preSaveTranslation(e){return e}preSaveTranslationKey(e,i){return i}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Hx=new y(""),SF=(()=>{class t{userConfig;constructor(e){this.userConfig=e}getNextLangs(){let e=this.userConfig.fallbackLang;if(!e)throw new Error("When using the default fallback, a fallback language must be provided in the config!");return Array.isArray(e)?e:[e]}static \u0275fac=function(i){return new(i||t)(V(Ra))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function Wl(t){if(!t)return"";let n=t.split("/");return n.pop(),n.join("/")}function mr(t){return t?t.split("/").pop():""}function G_(t,n,e="|"){if(ka(t)){let i=t.split(e),r=i.pop();return r===n?[!0,i.toString()]:[!1,r]}return[!1,""]}function zx(t,n){let[e]=G_(n,"static");return e?!1:!!t.config.reRenderOnLangChange}function Ux(t){return t?n=>n:Ce(1)}function MF(t,n){return Object.keys(t).reduce((e,i)=>(e[`${n}/${i}`]=t[i],e),{})}function q_(t,n){return yF(t)?MF(t.loader,n):void 0}function z_(t){return{scope:Wl(t)||null,langName:mr(t)}}function $x(t){let{path:n,inlineLoader:e,mainLoader:i,data:r}=t;if(e){let o=e[n];if(bF(o)===!1)throw`You're using an inline loader but didn't provide a loader for ${n}`;return e[n]().then(a=>a.default?a.default:a)}return i.getTranslation(n,r)}function TF({mainLoader:t,path:n,data:e,fallbackPath:i,inlineLoader:r}){return(i?[n,i]:[n]).map(a=>{let s=$x({path:a,mainLoader:t,inlineLoader:r,data:e});return Pe(s).pipe(N(l=>({translation:l,lang:a})))})}var kF;var Aa=(()=>{class t{loader;parser;missingHandler;interceptor;fallbackStrategy;langChanges$;translations=new Map;cache=new Map;firstFallbackLang;defaultLang="";availableLangs=[];isResolvedMissingOnce=!1;lang;failedLangs=new Set;events=new x;events$=this.events.asObservable();config;destroyRef=d(it);constructor(e,i,r,o,a,s){this.loader=e,this.parser=i,this.missingHandler=r,this.interceptor=o,this.fallbackStrategy=s,this.loader||(this.loader=new U_(this.translations)),kF=this,this.config=JSON.parse(JSON.stringify(a)),this.setAvailableLangs(this.config.availableLangs||[]),this.setFallbackLangForMissingTranslation(this.config),this.setDefaultLang(this.config.defaultLang),this.lang=new Je(this.getDefaultLang()),this.langChanges$=this.lang.asObservable(),this.events$.subscribe(l=>{l.type==="translationLoadSuccess"&&l.wasFailure&&this.setActiveLang(l.payload.langName)}),this.destroyRef.onDestroy(()=>{this.lang.complete(),this.events.complete(),this.cache.clear()})}getDefaultLang(){return this.defaultLang}setDefaultLang(e){this.defaultLang=e}getActiveLang(){return this.lang.getValue()}setActiveLang(e){return this.parser.onLangChanged?.(e),this.lang.next(e),this.events.next({type:"langChanged",payload:z_(e)}),this}setAvailableLangs(e){this.availableLangs=e}getAvailableLangs(){return this.availableLangs}load(e,i={}){let r=this.cache.get(e);if(r)return r;let o,a=this._isLangScoped(e),s;a&&(s=Wl(e));let l={path:e,mainLoader:this.loader,inlineLoader:i.inlineLoader,data:a?{scope:s}:void 0};if(this.useFallbackTranslation(e)){let u=a?`${s}/${this.firstFallbackLang}`:this.firstFallbackLang,f=TF(J(b({},l),{fallbackPath:u}));o=si(f)}else{let u=$x(l);o=Pe(u)}let c=o.pipe(Nm(this.config.failedRetries),tt(u=>{if(Array.isArray(u)){u.forEach(f=>{this.handleSuccess(f.lang,f.translation),f.lang!==e&&this.cache.set(f.lang,Z({}))});return}this.handleSuccess(e,u)}),zn(u=>(this.config.prodMode||console.error(`Error while trying to load "${e}"`,u),this.handleFailure(e,i))),Ar(1),V_(this.destroyRef));return this.cache.set(e,c),c}translate(e,i={},r=this.getActiveLang()){if(!e)return e;let{scope:o,resolveLang:a}=this.resolveLangAndScope(r);if(Array.isArray(e))return e.map(c=>this.translate(o?`${o}.${c}`:c,i,a));e=o?`${o}.${e}`:e;let s=this.getTranslation(a),l=s[e];return l?this.parser.transpile({value:l,params:i,translation:s,key:e}):this._handleMissingKey(e,l,i)}selectTranslate(e,i,r,o=!1){let a,s=(c,u)=>this.load(c,u).pipe(N(()=>o?this.translateObject(e,i,c):this.translate(e,i,c)));if($_(r))return this.langChanges$.pipe(Ee(c=>s(c)));if(r=Array.isArray(r)?r[0]:r,Lx(r)){let c=r;r=c.scope,a=q_(c,c.scope)}if(r=r,this.isLang(r)||this.isScopeWithLang(r))return s(r);let l=r;return this.langChanges$.pipe(Ee(c=>s(`${l}/${c}`,{inlineLoader:a})))}isScopeWithLang(e){return this.isLang(mr(e))}translateObject(e,i={},r=this.getActiveLang()){if(ka(e)||Array.isArray(e)){let{resolveLang:a,scope:s}=this.resolveLangAndScope(r);if(Array.isArray(e))return e.map(u=>this.translateObject(s?`${s}.${u}`:u,i,a));let l=this.getTranslation(a);e=s?`${s}.${e}`:e;let c=wF(this.getObjectByKey(l,e));return vF(c)?this.translate(e,i,r):this.parser.transpile({value:c,params:i,translation:l,key:e})}let o=[];for(let[a,s]of this.getEntries(e))o.push(this.translateObject(a,s,r));return o}selectTranslateObject(e,i,r){if(ka(e)||Array.isArray(e))return this.selectTranslate(e,i,r,!0);let[[o,a],...s]=this.getEntries(e);return this.selectTranslateObject(o,a,r).pipe(N(l=>{let c=[l];for(let[u,f]of s)c.push(this.translateObject(u,f,r));return c}))}getTranslation(e){if(e){if(this.isLang(e))return this.translations.get(e)||{};{let{scope:i,resolveLang:r}=this.resolveLangAndScope(e),o=this.translations.get(r)||{};return this.getObjectByKey(o,i)}}return this.translations}selectTranslation(e){let i=this.langChanges$;if(e){let r=mr(e)!==e;this.isLang(e)||r?i=Z(e):i=this.langChanges$.pipe(N(o=>`${e}/${o}`))}return i.pipe(Ee(r=>this.load(r).pipe(N(()=>this.getTranslation(r)))))}setTranslation(e,i=this.getActiveLang(),r={}){let a=b(b({},{merge:!0,emitChange:!0}),r),s=Wl(i),l=e;if(s){let _=this.getMappedScope(s);l=Ax({[_]:e})}let c=s?mr(i):i,u=b(b({},a.merge&&this.getTranslation(c)),l),f=this.config.flatten.aot?u:Ax(u),g=this.interceptor.preSaveTranslation(f,c);this.translations.set(c,g),a.emitChange&&this.setActiveLang(this.getActiveLang())}setTranslationKey(e,i,r={}){let o=r.lang||this.getActiveLang(),a=this.interceptor.preSaveTranslationKey(e,i,o),s={[e]:a};this.setTranslation(s,o,J(b({},r),{merge:!0}))}setFallbackLangForMissingTranslation({fallbackLang:e}){let i=Array.isArray(e)?e[0]:e;e&&this.useFallbackTranslation(i)&&(this.firstFallbackLang=i)}_handleMissingKey(e,i,r){if(this.config.missingHandler.allowEmpty&&i==="")return"";if(!this.isResolvedMissingOnce&&this.useFallbackTranslation()){this.isResolvedMissingOnce=!0;let o=this.translate(e,r,this.firstFallbackLang);return this.isResolvedMissingOnce=!1,o}return this.missingHandler.handle(e,this.getMissingHandlerData(),r)}_isLangScoped(e){return this.getAvailableLangsIds().indexOf(e)===-1}isLang(e){return this.getAvailableLangsIds().indexOf(e)!==-1}_loadDependencies(e,i){let r=mr(e);return this._isLangScoped(e)&&!this.isLoadedTranslation(r)?kr([this.load(r),this.load(e,{inlineLoader:i})]):this.load(e,{inlineLoader:i})}_completeScopeWithLang(e){return this._isLangScoped(e)&&!this.isLang(mr(e))?`${e}/${this.getActiveLang()}`:e}_setScopeAlias(e,i){this.config.scopeMapping||(this.config.scopeMapping={}),this.config.scopeMapping[e]=i}isLoadedTranslation(e){return Nx(this.getTranslation(e))}getAvailableLangsIds(){let e=this.getAvailableLangs()[0];return ka(e)?this.getAvailableLangs():this.getAvailableLangs().map(i=>i.id)}getMissingHandlerData(){return J(b({},this.config),{activeLang:this.getActiveLang(),availableLangs:this.availableLangs,defaultLang:this.defaultLang})}useFallbackTranslation(e){return this.config.missingHandler.useFallbackTranslation&&e!==this.firstFallbackLang}handleSuccess(e,i){this.setTranslation(i,e,{emitChange:!1}),this.events.next({wasFailure:!!this.failedLangs.size,type:"translationLoadSuccess",payload:z_(e)}),this.failedLangs.forEach(r=>this.cache.delete(r)),this.failedLangs.clear()}handleFailure(e,i){$_(i.failedCounter)&&(i.failedCounter=0,i.fallbackLangs||(i.fallbackLangs=this.fallbackStrategy.getNextLangs(e)));let r=e.split("/"),a=i.fallbackLangs[i.failedCounter];if(this.failedLangs.add(e),this.cache.has(a))return this.handleSuccess(a,this.getTranslation(a)),et;let s=a===r[r.length-1];if(!a||s){let c="Unable to load translation and all the fallback languages";throw r.length>1&&(c+=", did you misspelled the scope name?"),new Error(c)}let l=a;return r.length>1&&(r[r.length-1]=a,l=r.join("/")),i.failedCounter++,this.events.next({type:"translationLoadFailure",payload:z_(e)}),this.load(l,i)}getMappedScope(e){let{scopeMapping:i={},scopes:r={keepCasing:!1}}=this.config;return i[e]||(r.keepCasing?e:Fx(e))}resolveLangAndScope(e){let i=e,r;if(this._isLangScoped(e)){let o=mr(e),a=this.isLang(o);i=a?o:this.getActiveLang(),r=this.getMappedScope(a?Wl(e):e)}return{scope:r,resolveLang:i}}getObjectByKey(e,i){let r={},o=`${i}.`;for(let a in e)a.startsWith(o)&&(r[a.replace(o,"")]=e[a]);return r}getEntries(e){return e instanceof Map?e.entries():Object.entries(e)}static \u0275fac=function(i){return new(i||t)(V(Ox,8),V(Bx),V(jx),V(Vx),V(Ra),V(Hx))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),RF=(()=>{class t{html;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],inputs:{html:"html"},decls:1,vars:1,consts:[[1,"transloco-loader-template",3,"innerHTML"]],template:function(i,r){i&1&&Yt(0,"div",0),i&2&&Mt("innerHTML",r.html,$p)},encapsulation:2})}return t})(),W_=class{view;vcr;constructor(n,e){this.view=n,this.vcr=e}attachView(){if(this.view instanceof mt)this.vcr.createEmbeddedView(this.view);else if(ka(this.view)){let n=this.vcr.createComponent(RF);n.instance.html=this.view,n.hostView.detectChanges()}else this.vcr.createComponent(this.view)}detachView(){this.vcr.clear()}},Gx=new y(""),AF=new y(""),Wx=new y(""),xf=class{initialized=!1;resolve({inline:n,provider:e,active:i}){let r=i;if(this.initialized)return r=i,r;if(e){let[,o]=G_(e,"static");r=o}if(n){let[,o]=G_(n,"static");r=o}return this.initialized=!0,r}resolveLangBasedOnScope(n){return Wl(n)?mr(n):n}resolveLangPath(n,e){return e?`${e}/${n}`:n}},Ef=class{service;constructor(n){this.service=n}resolve(n){let{inline:e,provider:i}=n;if(e)return e;if(i){if(Lx(i)){let{scope:r,alias:o=this.service.config.scopes.keepCasing?r:Fx(r)}=i;return this.service._setScopeAlias(r,o),r}return i}}},hr=(()=>{class t{destroyRef=d(it);service=d(Aa);tpl=d(mt,{optional:!0});providerLang=d(Gx,{optional:!0});providerScope=d(Wx,{optional:!0});providedLoadingTpl=d(AF,{optional:!0});cdr=d(ye);host=d(L);vcr=d(ht);renderer=d(je);view;memo=new Map;key;params={};inlineScope;inlineRead;prefix;inlineLang;inlineTpl;currentLang;loaderTplHandler;initialized=!1;path;langResolver=new xf;scopeResolver=new Ef(this.service);strategy=this.tpl===null?"attribute":"structural";static ngTemplateContextGuard(e,i){return!0}ngOnInit(){let e=zx(this.service,this.providerLang||this.inlineLang);if(this.service.langChanges$.pipe(Ee(i=>{let r=this.langResolver.resolve({inline:this.inlineLang,provider:this.providerLang,active:i});return Array.isArray(this.providerScope)?si(this.providerScope.map(o=>this.resolveScope(r,o))):this.resolveScope(r,this.providerScope)}),Ux(e),V_(this.destroyRef)).subscribe(()=>{this.currentLang=this.langResolver.resolveLangBasedOnScope(this.path),this.strategy==="attribute"?this.attributeStrategy():this.structuralStrategy(this.currentLang,this.prefix||this.inlineRead),this.cdr.markForCheck(),this.initialized=!0}),!this.initialized){let i=this.resolveLoadingContent();i&&(this.loaderTplHandler=new W_(i,this.vcr),this.loaderTplHandler.attachView())}}ngOnChanges(e){this.strategy==="attribute"&&Object.keys(e).some(r=>!e[r].firstChange)&&this.attributeStrategy()}attributeStrategy(){this.detachLoader(),this.renderer.setProperty(this.host.nativeElement,"innerText",this.service.translate(this.key,this.params,this.currentLang))}structuralStrategy(e,i){this.memo.clear();let r=this.getTranslateFn(e,i);this.view?(this.view.context.$implicit=r,this.view.context.currentLang=this.currentLang):(this.detachLoader(),this.view=this.vcr.createEmbeddedView(this.tpl,{$implicit:r,currentLang:this.currentLang}))}getTranslateFn(e,i){return(r,o)=>{let a=i?`${i}.${r}`:r,s=o?`${a}${JSON.stringify(o)}`:a;return this.memo.has(s)||this.memo.set(s,this.service.translate(a,o,e)),this.memo.get(s)}}resolveLoadingContent(){return this.inlineTpl||this.providedLoadingTpl}ngOnDestroy(){this.memo.clear()}detachLoader(){this.loaderTplHandler?.detachView()}resolveScope(e,i){let r=this.scopeResolver.resolve({inline:this.inlineScope,provider:i});this.path=this.langResolver.resolveLangPath(e,r);let o=q_(i,r);return this.service._loadDependencies(this.path,o)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","transloco",""]],inputs:{key:[0,"transloco","key"],params:[0,"translocoParams","params"],inlineScope:[0,"translocoScope","inlineScope"],inlineRead:[0,"translocoRead","inlineRead"],prefix:[0,"translocoPrefix","prefix"],inlineLang:[0,"translocoLang","inlineLang"],inlineTpl:[0,"translocoLoadingTpl","inlineTpl"]},features:[qe]})}return t})(),Oa=(()=>{class t{service;providerScope;providerLang;cdr;subscription=null;lastValue="";lastKey;path;langResolver=new xf;scopeResolver;constructor(e,i,r,o){this.service=e,this.providerScope=i,this.providerLang=r,this.cdr=o,this.scopeResolver=new Ef(this.service)}transform(e,i,r){if(!e)return e;let o=i?`${e}${JSON.stringify(i)}`:e;if(o===this.lastKey)return this.lastValue;this.lastKey=o,this.subscription?.unsubscribe();let a=zx(this.service,this.providerLang||r);return this.subscription=this.service.langChanges$.pipe(Ee(s=>{let l=this.langResolver.resolve({inline:r,provider:this.providerLang,active:s});return Array.isArray(this.providerScope)?si(this.providerScope.map(c=>this.resolveScope(l,c))):this.resolveScope(l,this.providerScope)}),Ux(a)).subscribe(()=>this.updateValue(e,i)),this.lastValue}ngOnDestroy(){this.subscription?.unsubscribe(),this.subscription=null}updateValue(e,i){let r=this.langResolver.resolveLangBasedOnScope(this.path);this.lastValue=this.service.translate(e,i,r),this.cdr.markForCheck()}resolveScope(e,i){let r=this.scopeResolver.resolve({inline:void 0,provider:i});this.path=this.langResolver.resolveLangPath(e,r);let o=q_(i,r);return this.service._loadDependencies(this.path,o)}static \u0275fac=function(i){return new(i||t)(Ue(Aa,16),Ue(Wx,24),Ue(Gx,24),Ue(ye,16))};static \u0275pipe=au({name:"transloco",type:t,pure:!1})}return t})();var qx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})();function Qx(t){let n=[FF(DF),LF(EF),BF(IF),PF(SF)];return t.config&&n.push(OF(t.config)),t.loader&&n.push(NF(t.loader)),n}function OF(t){return xt([{provide:Ra,useValue:CF(t)}])}function NF(t){return xt([{provide:Ox,useClass:t}])}function FF(t){return xt([{provide:Bx,useClass:t,deps:[Ra]}])}function PF(t){return xt([{provide:Hx,useClass:t,deps:[Ra]}])}function LF(t){return xt([{provide:jx,useClass:t}])}function BF(t){return xt([{provide:Vx,useClass:t}])}function Yx(){let t=jF();if(!(!t||!Px()))return t.indexOf("-")!==-1&&(t=t.split("-")[0]),t.indexOf("_")!==-1&&(t=t.split("_")[0]),t}function jF(){if(!Px())return"";let t=window.navigator;return t.languages?.[0]??t.language}var Sf=class t{title="gwt-randomizers";swUpdate=d(rf);snackbar=d(kx);translocoService=d(Aa);ngOnInit(){this.swUpdate.unrecoverable.subscribe(n=>{this.snackbar.open(`An error occurred that we cannot recover from:
`+n.reason+`

Please reload the page.`,"Reload").onAction().subscribe(()=>{window.location.reload()}),console.debug(`An error occurred that we cannot recover from:
`+n.reason+`

Please reload the page.`)}),this.swUpdate.versionUpdates.pipe(ue(n=>n.type==="VERSION_DETECTED")).subscribe(()=>{this.snackbar.open(this.translocoService.translate("messages.update-available"),"Reload").onAction().subscribe(()=>{window.location.reload()})})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&re(0,"router-outlet")},dependencies:[wl,qx],encapsulation:2})};var Mf=class t{http=d(ia);getTranslation(n){return this.http.get(`i18n/${n}.json`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Tf=class t{playerCount=F(2);setPlayerCount(n){this.playerCount.set(n)}neutralBuildings=[{title:"A",sides:[{title:"front"}]},{title:"B",sides:[{title:"front"}]},{title:"C",sides:[{title:"front"}]},{title:"D",sides:[{title:"front"}]},{title:"E",sides:[{title:"front"}]},{title:"F",sides:[{title:"front"}]},{title:"G",sides:[{title:"front"}]},{title:"H",sides:[{title:"front"}]}];playerBuildings=[{title:"1",sides:[{title:"a"},{title:"b"}]},{title:"2",sides:[{title:"a"},{title:"b"}]},{title:"3",sides:[{title:"a"},{title:"b"}]},{title:"4",sides:[{title:"a"},{title:"b"}]},{title:"5",sides:[{title:"a"},{title:"b"}]},{title:"6",sides:[{title:"a"},{title:"b"}]},{title:"7",sides:[{title:"a"},{title:"b"}]},{title:"8",sides:[{title:"a"},{title:"b"}]},{title:"9",sides:[{title:"a"},{title:"b"}]},{title:"10",sides:[{title:"a"},{title:"b"}]}];stationMasters=[{title:"1",sides:[{title:"front",image:"img/gwt-argentina/station-master-01.png"}]},{title:"2",sides:[{title:"front",image:"img/gwt-argentina/station-master-02.png"}]},{title:"3",sides:[{title:"front",image:"img/gwt-argentina/station-master-03.png"}]},{title:"4",sides:[{title:"front",image:"img/gwt-argentina/station-master-04.png"}]},{title:"5",sides:[{title:"front",image:"img/gwt-argentina/station-master-05.png"}]},{title:"6",sides:[{title:"front",image:"img/gwt-argentina/station-master-06.png"}]},{title:"7",sides:[{title:"front",image:"img/gwt-argentina/station-master-07.png"}]},{title:"8",sides:[{title:"front",image:"img/gwt-argentina/station-master-08.png"}]}];cities=[{title:"Le Havre",sides:[{title:"a"},{title:"b"}]},{title:"Rotterdam",sides:[{title:"a"},{title:"b"}]},{title:"Liverpool",sides:[{title:"a"},{title:"b"}]}];getRandomNeutralBuildingOrder(){return this.shuffleArray(this.neutralBuildings)}getRandomStationMasters(){let n=[],e=this.shuffleArray(this.stationMasters);for(let i=0;i<5;i+=1)n.push(e.pop());return n}getRandomPlayerBuildings(){let n=JSON.parse(JSON.stringify(this.playerBuildings));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}getRandomCities(){let n=JSON.parse(JSON.stringify(this.cities));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}shuffleArray(n){let e=n.slice();for(let i,r,o=e.length;o;i=Math.floor(Math.random()*o),r=e[--o],e[o]=e[i],e[i]=r);return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Kx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[Ll]})}return t})();var ql=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new x;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Q_=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Zx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[Kx,se]})}return t})();var Y_=class{_box;_destroyed=new x;_resizeSubject=new x;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new oe(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ue(e=>e.some(i=>i.target===n)),Ar({bufferSize:1,refCount:!0}),fe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Xx=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(G);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Y_(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var VF=["notch"],HF=["matFormFieldNotchedOutline",""],zF=["*"],Jx=["iconPrefixContainer"],eE=["textPrefixContainer"],tE=["iconSuffixContainer"],nE=["textSuffixContainer"],UF=["textField"],$F=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],GF=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function WF(t,n){t&1&&re(0,"span",21)}function qF(t,n){if(t&1&&(h(0,"label",20),z(1,1),T(2,WF,1,0,"span",21),p()),t&2){let e=D(2);$("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ee("for",e._control.disableAutomaticLabeling?null:e._control.id),m(2),k(!e.hideRequiredMarker&&e._control.required?2:-1)}}function QF(t,n){if(t&1&&T(0,qF,3,5,"label",20),t&2){let e=D();k(e._hasFloatingLabel()?0:-1)}}function YF(t,n){t&1&&re(0,"div",7)}function KF(t,n){}function ZF(t,n){if(t&1&&He(0,KF,0,0,"ng-template",13),t&2){D(2);let e=Ct(1);$("ngTemplateOutlet",e)}}function XF(t,n){if(t&1&&(h(0,"div",9),T(1,ZF,1,1,null,13),p()),t&2){let e=D();$("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),m(),k(e._forceDisplayInfixLabel()?-1:1)}}function JF(t,n){t&1&&(h(0,"div",10,2),z(2,2),p())}function eP(t,n){t&1&&(h(0,"div",11,3),z(2,3),p())}function tP(t,n){}function nP(t,n){if(t&1&&He(0,tP,0,0,"ng-template",13),t&2){D();let e=Ct(1);$("ngTemplateOutlet",e)}}function iP(t,n){t&1&&(h(0,"div",14,4),z(2,4),p())}function rP(t,n){t&1&&(h(0,"div",15,5),z(2,5),p())}function oP(t,n){t&1&&re(0,"div",16)}function aP(t,n){t&1&&(h(0,"div",18),z(1,6),p())}function sP(t,n){if(t&1&&(h(0,"mat-hint",22),v(1),p()),t&2){let e=D(2);$("id",e._hintLabelId),m(),U(e.hintLabel)}}function lP(t,n){if(t&1&&(h(0,"div",19),T(1,sP,2,2,"mat-hint",22),z(2,7),re(3,"div",23),z(4,8),p()),t&2){let e=D();m(),k(e.hintLabel?1:-1)}}var pr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-label"]]})}return t})(),cP=new y("MatError");var Rf=(()=>{class t{align="start";id=d(Ve).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Mt("id",r.id),ee("align",null),P("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),dP=new y("MatPrefix");var uP=new y("MatSuffix");var cE=new y("FloatingLabelParent"),iE=(()=>{class t{_elementRef=d(L);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(Xx);_ngZone=d(G);_parent=d(cE);_resizeSubscription=new be;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return fP(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function fP(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var rE="mdc-line-ripple--active",kf="mdc-line-ripple--deactivating",oE=(()=>{class t{_elementRef=d(L);_cleanupTransitionEnd;constructor(){let e=d(G),i=d(je);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(kf),e.add(rE)}deactivate(){this._elementRef.nativeElement.classList.add(kf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(kf);e.propertyName==="opacity"&&r&&i.remove(rE,kf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),aE=(()=>{class t{_elementRef=d(L);_ngZone=d(G);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Re(VF,5),i&2){let o;q(o=Q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:HF,ngContentSelectors:zF,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(me(),Yt(0,"div",1),rt(1,"div",2,0),z(3),pt(),Yt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),K_=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t})}return t})();var Z_=new y("MatFormField"),mP=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),sE="fill",hP="auto",lE="fixed",pP="translateY(-50%)",Mi=(()=>{class t{_elementRef=d(L);_changeDetectorRef=d(ye);_platform=d(we);_idGenerator=d(Ve);_ngZone=d(G);_defaults=d(mP,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Xs("iconPrefixContainer");_textPrefixContainerSignal=Xs("textPrefixContainer");_iconSuffixContainerSignal=Xs("iconSuffixContainer");_textSuffixContainerSignal=Xs("textSuffixContainer");_prefixSuffixContainers=kt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=nC(pr);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Gt(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||hP}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||sE;this._appearanceSignal.set(i)}_appearanceSignal=F(sE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||lE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||lE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new x;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Oe();constructor(){let e=this._defaults,i=d(Ze);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Gi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=kt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Ge([void 0,void 0]),N(()=>[i.errorState,i.userAriaDescribedBy]),zc(),ue(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(fe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Bt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){oC({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=kt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${a+s}px`,_=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,C=`var(--mat-mdc-form-field-label-transform, ${pP} translateX(${_}))`,O=a+s+l+c;return[C,O]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(uu(o,r._labelChild,pr,5),Ke(o,K_,5)(o,dP,5)(o,uP,5)(o,cP,5)(o,Rf,5)),i&2){mu();let a;q(a=Q())&&(r._formFieldControl=a.first),q(a=Q())&&(r._prefixChildren=a),q(a=Q())&&(r._suffixChildren=a),q(a=Q())&&(r._errorChildren=a),q(a=Q())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(fu(r._iconPrefixContainerSignal,Jx,5)(r._textPrefixContainerSignal,eE,5)(r._iconSuffixContainerSignal,tE,5)(r._textSuffixContainerSignal,nE,5),Re(UF,5)(Jx,5)(eE,5)(tE,5)(nE,5)(iE,5)(aE,5)(oE,5)),i&2){mu(4);let o;q(o=Q())&&(r._textField=o.first),q(o=Q())&&(r._iconPrefixContainer=o.first),q(o=Q())&&(r._textPrefixContainer=o.first),q(o=Q())&&(r._iconSuffixContainer=o.first),q(o=Q())&&(r._textSuffixContainer=o.first),q(o=Q())&&(r._floatingLabel=o.first),q(o=Q())&&(r._notchedOutline=o.first),q(o=Q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&P("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ae([{provide:Z_,useExisting:t},{provide:cE,useExisting:t}])],ngContentSelectors:GF,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(me($F),He(0,QF,1,1,"ng-template",null,0,hu),h(2,"div",6,1),W("click",function(a){return r._control.onContainerClick(a)}),T(4,YF,1,0,"div",7),h(5,"div",8),T(6,XF,2,2,"div",9),T(7,JF,3,0,"div",10),T(8,eP,3,0,"div",11),h(9,"div",12),T(10,nP,1,1,null,13),z(11),p(),T(12,iP,3,0,"div",14),T(13,rP,3,0,"div",15),p(),T(14,oP,1,0,"div",16),p(),h(15,"div",17),T(16,aP,2,0,"div",18)(17,lP,5,1,"div",19),p()),i&2){let o;m(2),P("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),m(2),k(!r._hasOutline()&&!r._control.disabled?4:-1),m(2),k(r._hasOutline()?6:-1),m(),k(r._hasIconPrefix?7:-1),m(),k(r._hasTextPrefix?8:-1),m(2),k(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),m(2),k(r._hasTextSuffix?12:-1),m(),k(r._hasIconSuffix?13:-1),m(),k(r._hasOutline()?-1:14),m(),P("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();m(),k((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[iE,aE,Ig,oE,Rf],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var gE=new y("");function ev(t){return t==null||tv(t)===0}function tv(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var $f=new y(""),_E=new y(""),gP=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Of=class{static min(n){return _P(n)}static max(n){return vP(n)}static required(n){return bP(n)}static requiredTrue(n){return yP(n)}static email(n){return wP(n)}static minLength(n){return CP(n)}static maxLength(n){return DP(n)}static pattern(n){return xP(n)}static nullValidator(n){return vE()}static compose(n){return xE(n)}static composeAsync(n){return EE(n)}};function _P(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function vP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function bP(t){return ev(t.value)?{required:!0}:null}function yP(t){return t.value===!0?null:{required:!0}}function wP(t){return ev(t.value)||gP.test(t.value)?null:{email:!0}}function CP(t){return n=>{let e=n.value?.length??tv(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function DP(t){return n=>{let e=n.value?.length??tv(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function xP(t){if(!t)return vE;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(ev(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function vE(t){return null}function bE(t){return t!=null}function yE(t){return Ki(t)?Pe(t):t}function wE(t){let n={};return t.forEach(e=>{n=e!=null?b(b({},n),e):n}),Object.keys(n).length===0?null:n}function CE(t,n){return n.map(e=>e(t))}function EP(t){return!t.validate}function DE(t){return t.map(n=>EP(n)?n:e=>n.validate(e))}function xE(t){if(!t)return null;let n=t.filter(bE);return n.length==0?null:function(e){return wE(CE(e,n))}}function nv(t){return t!=null?xE(DE(t)):null}function EE(t){if(!t)return null;let n=t.filter(bE);return n.length==0?null:function(e){let i=CE(e,n).map(yE);return si(i).pipe(N(wE))}}function iv(t){return t!=null?EE(DE(t)):null}function dE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function IE(t){return t._rawValidators}function SE(t){return t._rawAsyncValidators}function X_(t){return t?Array.isArray(t)?t:[t]:[]}function Nf(t,n){return Array.isArray(t)?t.includes(n):t===n}function uE(t,n){let e=X_(n);return X_(t).forEach(r=>{Nf(e,r)||e.push(r)}),e}function fE(t,n){return X_(n).filter(e=>!Nf(t,e))}var Ff=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=nv(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=iv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Pa=class extends Ff{name;get formDirective(){return null}get path(){return null}},Pf=class extends Ff{_parent=null;name=null;valueAccessor=null};var Ql="VALID",Af="INVALID",Na="PENDING",Yl="DISABLED",gr=class{},Lf=class extends gr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Zl=class extends gr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Xl=class extends gr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Fa=class extends gr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Bf=class extends gr{source;constructor(n){super(),this.source=n}},jf=class extends gr{source;constructor(n){super(),this.source=n}};function ME(t){return(Gf(t)?t.validators:t)||null}function IP(t){return Array.isArray(t)?nv(t):t||null}function TE(t,n){return(Gf(n)?n.asyncValidators:t)||null}function SP(t){return Array.isArray(t)?iv(t):t||null}function Gf(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function MP(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new S(1e3,"");if(!i[e])throw new S(1001,"")}function TP(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new S(-1002,"")})}var Vf=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ie(this.statusReactive)}set status(n){Ie(()=>this.statusReactive.set(n))}_status=kt(()=>this.statusReactive());statusReactive=F(void 0);get valid(){return this.status===Ql}get invalid(){return this.status===Af}get pending(){return this.status===Na}get disabled(){return this.status===Yl}get enabled(){return this.status!==Yl}errors;get pristine(){return Ie(this.pristineReactive)}set pristine(n){Ie(()=>this.pristineReactive.set(n))}_pristine=kt(()=>this.pristineReactive());pristineReactive=F(!0);get dirty(){return!this.pristine}get touched(){return Ie(this.touchedReactive)}set touched(n){Ie(()=>this.touchedReactive.set(n))}_touched=kt(()=>this.touchedReactive());touchedReactive=F(!1);get untouched(){return!this.touched}_events=new x;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(uE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(uE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(fE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(fE(n,this._rawAsyncValidators))}hasValidator(n){return Nf(this._rawValidators,n)}hasAsyncValidator(n){return Nf(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(J(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Xl(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Xl(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(J(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Zl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Zl(!0,i))}markAsPending(n={}){this.status=Na;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Fa(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(J(b({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Yl,this.errors=null,this._forEachChild(r=>{r.disable(J(b({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lf(this.value,i)),this._events.next(new Fa(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(J(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Ql,this._forEachChild(i=>{i.enable(J(b({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(J(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Ql||this.status===Na)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lf(this.value,e)),this._events.next(new Fa(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(J(b({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Yl:Ql}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Na,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=yE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Fa(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new X,this.statusChanges=new X}_calculateStatus(){return this._allControlsDisabled()?Yl:this.errors?Af:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Na)?Na:this._anyControlsHaveStatus(Af)?Af:Ql}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Zl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Xl(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Gf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=IP(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=SP(this._rawAsyncValidators)}},Hf=class extends Vf{constructor(n,e,i){super(ME(e),TE(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){TP(this,!0,n),Object.keys(n).forEach(i=>{MP(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,J(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new jf(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var rv=new y("",{factory:()=>ov}),ov="always";function J_(t,n,e=ov){av(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),RP(t,n),OP(t,n),AP(t,n),kP(t,n)}function mE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Uf(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function zf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function kP(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function av(t,n){let e=IE(t);n.validator!==null?t.setValidators(dE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=SE(t);n.asyncValidator!==null?t.setAsyncValidators(dE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();zf(n._rawValidators,r),zf(n._rawAsyncValidators,r)}function Uf(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=IE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=SE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return zf(n._rawValidators,i),zf(n._rawAsyncValidators,i),e}function RP(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&kE(t,n)})}function AP(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&kE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function kE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function OP(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function RE(t,n){t==null,av(t,n)}function NP(t,n){return Uf(t,n)}function AE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function FP(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var PP={provide:Pa,useExisting:wn(()=>sv)},Kl=Promise.resolve(),sv=(()=>{class t extends Pa{callSetDisabledState;get submitted(){return Ie(this.submittedReactive)}_submitted=kt(()=>this.submittedReactive());submittedReactive=F(!1);_directives=new Set;form;ngSubmit=new X;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Hf({},nv(e),iv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Kl.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),J_(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Kl.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Kl.then(()=>{let i=this._findContainer(e.path),r=new Hf({});RE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Kl.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Kl.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),AE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Bf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(Ue($f,10),Ue(_E,10),Ue(rv,8))};static \u0275dir=H({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&W("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ae([PP]),De]})}return t})();function hE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function pE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var LP=class extends Vf{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(ME(e),TE(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Gf(e)&&(e.nonNullable||e.initialValueIsDefault)&&(pE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new jf(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){hE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){hE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){pE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var BP=t=>t instanceof LP;var jP=(()=>{class t extends Pa{callSetDisabledState;get submitted(){return Ie(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=kt(()=>this._submittedReactive());_submittedReactive=F(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Uf(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return J_(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){mE(e.control||null,e,!1),FP(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,AE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Bf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(mE(i||null,e),BP(r)&&(J_(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);RE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&NP(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){av(this.form,this),this._oldForm&&Uf(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(Ue($f,10),Ue(_E,10),Ue(rv,8))};static \u0275dir=H({type:t,features:[De,qe]})}return t})();var VP={provide:Pa,useExisting:wn(()=>lv)},lv=(()=>{class t extends jP{form=null;ngSubmit=new X;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&W("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ae([VP]),De]})}return t})();var HP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})();var OE=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:rv,useValue:e.callSetDisabledState??ov}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[HP]})}return t})();var NE=(()=>{class t{_animationsDisabled=Oe();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&P("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var zP=["text"],UP=[[["mat-icon"]],"*"],$P=["mat-icon","*"];function GP(t,n){if(t&1&&re(0,"mat-pseudo-checkbox",1),t&2){let e=D();$("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function WP(t,n){if(t&1&&re(0,"mat-pseudo-checkbox",3),t&2){let e=D();$("disabled",e.disabled)}}function qP(t,n){if(t&1&&(h(0,"span",4),v(1),p()),t&2){let e=D();m(),I("(",e.group.label,")")}}var dv=new y("MAT_OPTION_PARENT_COMPONENT"),uv=new y("MatOptgroup");var cv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},ri=(()=>{class t{_element=d(L);_changeDetectorRef=d(ye);_parent=d(dv,{optional:!0});group=d(uv,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ve).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=F(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new X;_text;_stateChanges=new x;constructor(){let e=d(lt);e.load(ni),e.load(va),this._signalDisableRipple=!!this._parent&&Yi(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!ct(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new cv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Re(zP,7),i&2){let o;q(o=Q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&W("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(Mt("id",r.id),ee("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),P("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",te]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:$P,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(me(UP),T(0,GP,1,2,"mat-pseudo-checkbox",1),z(1),h(2,"span",2,0),z(4,1),p(),T(5,WP,1,1,"mat-pseudo-checkbox",3),T(6,qP,2,1,"span",4),re(7,"div",5)),i&2&&(k(r.multiple?0:-1),m(5),k(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),m(),k(r.group&&r.group._inert?6:-1),m(),$("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[NE,ur],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function FE(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function PE(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var LE=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Wf=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var BE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[wa,Mi,se]})}return t})();var qf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var fv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[ii,qf,ri,se]})}return t})();var ZP=["trigger"],XP=["panel"],JP=[[["mat-select-trigger"]],"*"],eL=["mat-select-trigger","*"];function tL(t,n){if(t&1&&(h(0,"span",4),v(1),p()),t&2){let e=D();m(),U(e.placeholder)}}function nL(t,n){t&1&&z(0)}function iL(t,n){if(t&1&&(h(0,"span",11),v(1),p()),t&2){let e=D(2);m(),U(e.triggerValue)}}function rL(t,n){if(t&1&&(h(0,"span",5),T(1,nL,1,0)(2,iL,2,1,"span",11),p()),t&2){let e=D();m(),k(e.customTrigger?1:2)}}function oL(t,n){if(t&1){let e=$e();h(0,"div",12,1),W("keydown",function(r){_e(e);let o=D();return ve(o._handleKeydown(r))}),z(2,1),p()}if(t&2){let e=D();st(e.panelClass),P("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),ee("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var aL=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Ii(t)}}),sL=new y("MAT_SELECT_CONFIG"),lL=new y("MatSelectTrigger"),mv=class{source;value;constructor(n,e){this.source=n,this.value=e}},ja=(()=>{class t{_viewportRuler=d(hn);_changeDetectorRef=d(ye);_elementRef=d(L);_dir=d(Ze,{optional:!0});_idGenerator=d(Ve);_renderer=d(je);_parentFormField=d(Z_,{optional:!0});ngControl=d(Pf,{self:!0,optional:!0});_liveAnnouncer=d(Tl);_defaultOptions=d(sL,{optional:!0});_animationsDisabled=Oe();_popoverLocation;_initialized=new x;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=FE(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=PE(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new mv(this,e)}_scrollStrategyFactory=d(aL);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new x;_errorStateTracker;stateChanges=new x;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=F(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Of.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=bn(()=>{let e=this.options;return e?e.changes.pipe(Ge(e),Ee(()=>Bt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Ee(()=>this.optionSelectionChanges))});openedChange=new X;_openedStream=this.openedChange.pipe(ue(e=>e),N(()=>{}));_closedStream=this.openedChange.pipe(ue(e=>!e),N(()=>{}));selectionChange=new X;valueChange=new X;constructor(){let e=d(LE),i=d(sv,{optional:!0}),r=d(lv,{optional:!0}),o=d(new un("tabindex"),{optional:!0}),a=d(Hl,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Wf(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new ql(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(fe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(fe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Ge(null),fe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ce(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&ff(this._trackedModal,"aria-owns",i),I_(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;ff(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!ct(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!ct(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!ct(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ia?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Fl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Bt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(fe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Bt(...this.options.map(i=>i._stateChanges)).pipe(fe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Rt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,lL,5)(o,ri,5)(o,uv,5),i&2){let a;q(a=Q())&&(r.customTrigger=a.first),q(a=Q())&&(r.options=a),q(a=Q())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&Re(ZP,5)(XP,5)(Cf,5),i&2){let o;q(o=Q())&&(r.trigger=o.first),q(o=Q())&&(r.panel=o.first),q(o=Q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&W("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ee("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),P("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",te],disableRipple:[2,"disableRipple","disableRipple",te],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Ut(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",te],placeholder:"placeholder",required:[2,"required","required",te],multiple:[2,"multiple","multiple",te],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",te],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Ut],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",te]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Ae([{provide:K_,useExisting:t},{provide:dv,useExisting:t}]),qe],ngContentSelectors:eL,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(me(JP),h(0,"div",2,0),W("click",function(){return r.open()}),h(3,"div",3),T(4,tL,2,1,"span",4)(5,rL,3,1,"span",5),p(),h(6,"div",6)(7,"div",7),qt(),h(8,"svg",8),re(9,"path",9),p()()()(),He(10,oL,3,16,"ng-template",10),W("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=Ct(1);m(3),ee("id",r._valueId),m(),k(r.empty?4:5),m(6),$("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ia,Cf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var Qf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[pn,fv,se,mn,BE,fv]})}return t})();var cL=["tooltip"],dL=20;var uL=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Ii(t,{scrollThrottle:dL})}}),fL=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var VE="tooltip-panel",mL={passive:!0},hL=8,pL=8,gL=24,_L=200,HE=(()=>{class t{_elementRef=d(L);_ngZone=d(G);_platform=d(we);_ariaDescriber=d(nx);_focusMonitor=d(Ft);_dir=d(Ze);_injector=d(j);_viewContainerRef=d(ht);_mediaMatcher=d(ya);_document=d(K);_renderer=d(je);_animationsDisabled=Oe();_defaultOptions=d(fL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=vL;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Gt(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Gt(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=$t(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=$t(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new x;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=hL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(fe(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Fn(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(fe(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof L)return this._overlayRef;this._detach()}let i=this._injector.get(ti).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${VE}`,o=go(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(fe(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=jn(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(uL)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(fe(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(fe(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(fe(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(fe(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(b(b({},r.main),o.main)),this._addOffset(b(b({},r.fallback),o.fallback))])}_addOffset(e){let i=pL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),Be(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,a;if(i==="center"?this._dir&&this._dir.value==="rtl"?a=r==="end"?"left":"right":a=r==="start"?"left":"right":a=i==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let l=`${this._cssClassPrefix}-${VE}-`;s.removePanelClass(l+this._currentPosition),s.addPanelClass(l+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,mL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||Be({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!ct(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),vL=(()=>{class t{_changeDetectorRef=d(ye);_elementRef=d(L);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Oe();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new x;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>gL&&e.width>=_L}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(i);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Re(cL,7),i&2){let o;q(o=Q())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&W("mouseleave",function(a){return r._handleMouseLeave(a)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(rt(0,"div",1,0),Ko("animationend",function(a){return r._handleAnimationEnd(a)}),rt(2,"div",2),v(3),pt()()),i&2&&(st(r.tooltipClass),P("mdc-tooltip--multiline",r._isMultiline),m(3),U(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return t})();var zE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[kl,pn,se,mn]})}return t})();function bL(t,n){if(t&1&&(h(0,"mat-option",17),v(1),p()),t&2){let e=n.$implicit;$("value",e),m(),I(" ",e," ")}}function yL(t,n){if(t&1){let e=$e();h(0,"mat-form-field",14)(1,"mat-select",16,0),W("selectionChange",function(r){_e(e);let o=D(2);return ve(o._changePageSize(r.value))}),It(3,bL,2,2,"mat-option",17,dn),p(),h(5,"div",18),W("click",function(){_e(e);let r=Ct(2);return ve(r.open())}),p()()}if(t&2){let e=D(2);$("appearance",e._formFieldAppearance)("color",e.color),m(),$("value",e.pageSize)("disabled",e.disabled),qs("aria-labelledby",e._pageSizeLabelId),$("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),m(2),St(e._displayedPageSizeOptions)}}function wL(t,n){if(t&1&&(h(0,"div",15),v(1),p()),t&2){let e=D(2);m(),U(e.pageSize)}}function CL(t,n){if(t&1&&(h(0,"div",3)(1,"div",13),v(2),p(),T(3,yL,6,7,"mat-form-field",14),T(4,wL,2,1,"div",15),p()),t&2){let e=D();m(),ee("id",e._pageSizeLabelId),m(),I(" ",e._intl.itemsPerPageLabel," "),m(),k(e._displayedPageSizeOptions.length>1?3:-1),m(),k(e._displayedPageSizeOptions.length<=1?4:-1)}}function DL(t,n){if(t&1){let e=$e();h(0,"button",19),W("click",function(){_e(e);let r=D();return ve(r._buttonClicked(0,r._previousButtonsDisabled()))}),qt(),h(1,"svg",8),re(2,"path",20),p()()}if(t&2){let e=D();$("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),ee("aria-label",e._intl.firstPageLabel)}}function xL(t,n){if(t&1){let e=$e();h(0,"button",21),W("click",function(){_e(e);let r=D();return ve(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),qt(),h(1,"svg",8),re(2,"path",22),p()()}if(t&2){let e=D();$("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),ee("aria-label",e._intl.lastPageLabel)}}var EL=(()=>{class t{changes=new x;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,a=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${a} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),IL=50;var SL=new y("MAT_PAGINATOR_DEFAULT_OPTIONS"),ML=(()=>{class t{_intl=d(EL);_changeDetectorRef=d(ye);_formFieldAppearance;_pageSizeLabelId=d(Ve).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new vn(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>Ut(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new X;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(SL,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:a,showFirstLastButtons:s}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),a!=null&&(this.hidePageSize=a),s!=null&&(this.showFirstLastButtons=s)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:IL),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",Ut],length:[2,"length","length",Ut],pageSize:[2,"pageSize","pageSize",Ut],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",te],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",te],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",te]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(h(0,"div",1)(1,"div",2),T(2,CL,5,4,"div",3),h(3,"div",4)(4,"div",5),v(5),p(),T(6,DL,3,5,"button",6),h(7,"button",7),W("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),qt(),h(8,"svg",8),re(9,"path",9),p()(),xs(),h(10,"button",10),W("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),qt(),h(11,"svg",8),re(12,"path",11),p()(),T(13,xL,3,5,"button",12),p()()()),i&2&&(m(2),k(r.hidePageSize?-1:2),m(3),I(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),m(),k(r.showFirstLastButtons?6:-1),m(),$("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),ee("aria-label",r._intl.previousPageLabel),m(3),$("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),ee("aria-label",r._intl.nextPageLabel),m(3),k(r.showFirstLastButtons?13:-1))},dependencies:[Mi,ja,ri,_o,HE],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2,changeDetection:0})}return t})(),UE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[vo,Qf,zE,ML]})}return t})();var TL=["*",[["mat-toolbar-row"]]],kL=["*","mat-toolbar-row"],RL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),Yf=(()=>{class t{_elementRef=d(L);_platform=d(we);_document=d(K);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,RL,5),i&2){let a;q(a=Q())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(st(r.color?"mat-"+r.color:""),P("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:kL,decls:2,vars:0,template:function(i,r){i&1&&(me(TL),z(0),z(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var $E=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var WE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var Zf=["*"],AL=["content"],OL=[[["mat-drawer"]],[["mat-drawer-content"]],"*"],NL=["mat-drawer","mat-drawer-content","*"];function FL(t,n){if(t&1){let e=$e();h(0,"div",1),W("click",function(){_e(e);let r=D();return ve(r._onBackdropClicked())}),p()}if(t&2){let e=D();P("mat-drawer-shown",e._isShowingBackdrop())}}function PL(t,n){t&1&&(h(0,"mat-drawer-content"),z(1,2),p())}var LL=[[["mat-sidenav"]],[["mat-sidenav-content"]],"*"],BL=["mat-sidenav","mat-sidenav-content","*"];function jL(t,n){if(t&1){let e=$e();h(0,"div",1),W("click",function(){_e(e);let r=D();return ve(r._onBackdropClicked())}),p()}if(t&2){let e=D();P("mat-drawer-shown",e._isShowingBackdrop())}}function VL(t,n){t&1&&(h(0,"mat-sidenav-content"),z(1,2),p())}var HL=`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`;var zL=new y("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),gv=new y("MAT_DRAWER_CONTAINER"),Kf=(()=>{class t extends Ei{_platform=d(we);_changeDetectorRef=d(ye);_container=d(pv);constructor(){let e=d(L),i=d(ti),r=d(G);super(e,i,r)}ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()})}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:e,end:i}=this._container;return e!=null&&e.mode!=="over"&&e.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(Tt("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),P("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[Ae([{provide:Ei,useExisting:t}]),De],ngContentSelectors:Zf,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},encapsulation:2,changeDetection:0})}return t})(),hv=(()=>{class t{_elementRef=d(L);_focusTrapFactory=d(Ml);_focusMonitor=d(Ft);_platform=d(we);_ngZone=d(G);_renderer=d(je);_interactivityChecker=d(Ca);_doc=d(K);_container=d(gv,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(e){e=e==="end"?"end":"start",e!==this._position&&(this._isAttached&&this._updatePositionInParent(e),this._position=e,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(e){this._mode=e,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(e){this._disableClose=Gt(e)}_disableClose=!1;get autoFocus(){let e=this._autoFocus;return e??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(e){(e==="true"||e==="false"||e==null)&&(e=Gt(e)),this._autoFocus=e}_autoFocus;get opened(){return this._opened()}set opened(e){this.toggle(Gt(e))}_opened=F(!1);_openedVia=null;_animationStarted=new x;_animationEnd=new x;openedChange=new X(!0);_openedStream=this.openedChange.pipe(ue(e=>e),N(()=>{}));openedStart=this._animationStarted.pipe(ue(()=>this.opened),jc(void 0));_closedStream=this.openedChange.pipe(ue(e=>!e),N(()=>{}));closedStart=this._animationStarted.pipe(ue(()=>!this.opened),jc(void 0));_destroyed=new x;onPositionChanged=new X;_content;_modeChanged=new x;_injector=d(j);_changeDetectorRef=d(ye);constructor(){this.openedChange.pipe(fe(this._destroyed)).subscribe(e=>{e?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let e=this._renderer,i=this._elementRef.nativeElement;return[e.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!ct(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),e.listen(i,"transitionend",this._handleTransitionEvent),e.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_takeFocus(){if(!this._focusTrap)return;let e=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":Be(()=>{!this._focusTrap.focusInitialElement()&&typeof e.focus=="function"&&e.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(e){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,e):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let e=this._doc.activeElement;return!!e&&this._elementRef.nativeElement.contains(e)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(e){return this.toggle(!0,e)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(e=!this.opened,i){e&&i&&(this._openedVia=i);let r=this._setOpen(e,!e&&this._isFocusWithinDrawer(),this._openedVia||"program");return e||(this._openedVia=null),r}_setOpen(e,i,r){return e===this.opened?Promise.resolve(e?"open":"close"):(this._opened.set(e),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",e),!e&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(Ce(1)).subscribe(a=>o(a?"open":"close"))}))}_setIsAnimating(e){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",e)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(e){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;e==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=e=>{let i=this._elementRef.nativeElement;e.target===i&&this._ngZone.run(()=>{e.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(e)})};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&Re(AL,5),i&2){let o;q(o=Q())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(ee("align",null)("tabIndex",r.mode!=="side"?"-1":null),Tt("visibility",!r._container&&!r.opened?"hidden":null),P("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:Zf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(me(),h(0,"div",1,0),z(2),p())},dependencies:[Ei],encapsulation:2,changeDetection:0})}return t})(),pv=(()=>{class t{_dir=d(Ze,{optional:!0});_element=d(L);_ngZone=d(G);_changeDetectorRef=d(ye);_animationDisabled=Oe();_transitionsEnabled=!1;_allDrawers;_drawers=new Tn;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(e){this._autosize=Gt(e)}_autosize=d(zL);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(e){this._backdropOverride=e==null?null:Gt(e)}_backdropOverride=null;backdropClick=new X;_start=null;_end=null;_left=null;_right=null;_destroyed=new x;_doCheckSubject=new x;_contentMargins={left:null,right:null};_contentMarginChanges=new x;get scrollable(){return this._userContent||this._content}_injector=d(j);constructor(){let e=d(we),i=d(hn);this._dir?.change.pipe(fe(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(fe(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&e.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(Ge(this._allDrawers),fe(this._destroyed)).subscribe(e=>{this._drawers.reset(e.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(Ge(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(e=>{this._watchDrawerToggle(e),this._watchDrawerPosition(e),this._watchDrawerMode(e)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe(Rr(10),fe(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(e=>e.open())}close(){this._drawers.forEach(e=>e.close())}updateContentMargins(){let e=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")e+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();e+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,e-=r}}e=e||null,i=i||null,(e!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:e,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(e){e._animationStarted.pipe(fe(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),e.mode!=="side"&&e.openedChange.pipe(fe(this._drawers.changes)).subscribe(()=>this._setContainerClass(e.opened))}_watchDrawerPosition(e){e.onPositionChanged.pipe(fe(this._drawers.changes)).subscribe(()=>{Be({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(e){e._modeChanged.pipe(fe(Bt(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(e){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";e?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(e=>{e.position=="end"?(this._end!=null,this._end=e):(this._start!=null,this._start=e)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(e=>e&&!e.disableClose&&this._drawerHasBackdrop(e)).forEach(e=>e._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(e){return e!=null&&e.opened}_drawerHasBackdrop(e){return this._backdropOverride==null?!!e&&e.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,Kf,5)(o,hv,5),i&2){let a;q(a=Q())&&(r._content=a.first),q(a=Q())&&(r._allDrawers=a)}},viewQuery:function(i,r){if(i&1&&Re(Kf,5),i&2){let o;q(o=Q())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[Ae([{provide:gv,useExisting:t}])],ngContentSelectors:NL,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(me(OL),T(0,FL,1,2,"div",0),z(1),z(2,1),T(3,PL,2,0,"mat-drawer-content")),i&2&&(k(r.hasBackdrop?0:-1),m(3),k(r._content?-1:3))},dependencies:[Kf],styles:[`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`],encapsulation:2,changeDetection:0})}return t})(),Va=(()=>{class t extends Kf{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[Ae([{provide:Ei,useExisting:t}]),De],ngContentSelectors:Zf,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},encapsulation:2,changeDetection:0})}return t})(),Jl=(()=>{class t extends hv{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(e){this._fixedInViewport=Gt(e)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(e){this._fixedTopGap=$t(e)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(e){this._fixedBottomGap=$t(e)}_fixedBottomGap=0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(ee("tabIndex",r.mode!=="side"?"-1":null)("align",null),Tt("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),P("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[Ae([{provide:hv,useExisting:t}]),De],ngContentSelectors:Zf,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(me(),h(0,"div",1,0),z(2),p())},dependencies:[Ei],encapsulation:2,changeDetection:0})}return t})(),Xf=(()=>{class t extends pv{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,Va,5)(o,Jl,5),i&2){let a;q(a=Q())&&(r._content=a.first),q(a=Q())&&(r._allDrawers=a)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[Ae([{provide:gv,useExisting:t},{provide:pv,useExisting:t}]),De],ngContentSelectors:BL,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(me(LL),T(0,jL,1,2,"div",0),z(1),z(2,1),T(3,VL,2,0,"mat-sidenav-content")),i&2&&(k(r.hasBackdrop?0:-1),m(3),k(r._content?-1:3))},dependencies:[Va],styles:[HL],encapsulation:2,changeDetection:0})}return t})(),qE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[mn,se,mn]})}return t})();function YE(t){return Error(`Unable to find icon with the name "${t}"`)}function UL(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function KE(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function ZE(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Ti=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},JE=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Ti(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(at.HTML,r);if(!a)throw ZE(r);let s=fo(a);return this._addSvgIconConfig(e,i,new Ti("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Ti(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(at.HTML,i);if(!o)throw ZE(i);let a=fo(o);return this._addSvgIconSetConfig(e,new Ti("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(at.RESOURCE_URL,e);if(!i)throw KE(e);let r=this._cachedIconsByUrl.get(i);return r?Z(Jf(r)):this._loadSvgIconFromConfig(new Ti(e,null)).pipe(tt(o=>this._cachedIconsByUrl.set(i,o)),N(o=>Jf(o)))}getNamedSvgIcon(e,i=""){let r=XE(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):rs(YE(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Z(Jf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(N(i=>Jf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Z(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(zn(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(at.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),Z(null)})));return si(o).pipe(N(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw YE(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(tt(i=>e.svgText=i),N(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Z(null):this._fetchIcon(e).pipe(tt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(fo("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(fo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw UL();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(at.RESOURCE_URL,i);if(!a)throw KE(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(N(c=>fo(c)),Fi(()=>this._inProgressUrlFetches.delete(a)),as());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(XE(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return $L(o)?new Ti(o.url,null,o.options):new Ti(o,null)}}static \u0275fac=function(i){return new(i||t)(V(ia,8),V(al),V(K,8),V(Vt))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Jf(t){return t.cloneNode(!0)}function XE(t,n){return t+":"+n}function $L(t){return!!(t.url&&t.options)}var GL=["*"],WL=new y("MAT_ICON_DEFAULT_OPTIONS"),qL=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(K),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),eI=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],QL=eI.map(t=>`[${t}]`).join(", "),YL=/^url\(['"]?#(.*?)['"]?\)$/,em=(()=>{class t{_elementRef=d(L);_iconRegistry=d(JE);_location=d(qL);_errorHandler=d(Vt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=be.EMPTY;constructor(){let e=d(new un("aria-hidden"),{optional:!0}),i=d(WL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(QL),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)eI.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(YL):null;if(c){let u=r.get(s);u||(u=[],r.set(s,u)),u.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ce(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(ee("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),st(r.color?"mat-"+r.color:""),P("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",te],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:GL,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),tI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var ec=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=Gt(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=Gt(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(ee("aria-orientation",r.vertical?"vertical":"horizontal"),P("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})(),tm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var iI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[wa,ii,qf,se,tm]})}return t})();var KL=["mat-internal-form-field",""],ZL=["*"],nm=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:KL,ngContentSelectors:ZL,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var XL=["switch"],JL=["*"];function e2(t,n){t&1&&(h(0,"span",11),qt(),h(1,"svg",13),re(2,"path",14),p(),h(3,"svg",15),re(4,"path",16),p()())}var t2=new y("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),im=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},_v=(()=>{class t{_elementRef=d(L);_focusMonitor=d(Ft);_changeDetectorRef=d(ye);defaults=d(t2);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new im(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Oe();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new X;toggleChange=new X;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(lt).load(ni);let e=d(new un("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(Ve).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new im(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&Re(XL,5),i&2){let o;q(o=Q())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Mt("id",r.id),ee("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),st(r.color?"mat-"+r.color:""),P("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",te],color:"color",disabled:[2,"disabled","disabled",te],disableRipple:[2,"disableRipple","disableRipple",te],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Ut(e)],checked:[2,"checked","checked",te],hideIcon:[2,"hideIcon","hideIcon",te],disabledInteractive:[2,"disabledInteractive","disabledInteractive",te]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[Ae([{provide:gE,useExisting:wn(()=>t),multi:!0},{provide:$f,useExisting:t,multi:!0}]),qe],ngContentSelectors:JL,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(me(),h(0,"div",1)(1,"button",2,0),W("click",function(){return r._handleClick()}),re(3,"div",3)(4,"span",4),h(5,"span",5)(6,"span",6)(7,"span",7),re(8,"span",8),p(),h(9,"span",9),re(10,"span",10),p(),T(11,e2,5,0,"span",11),p()()(),h(12,"label",12),W("click",function(a){return a.stopPropagation()}),z(13),p()()),i&2){let o=Ct(2);$("labelPosition",r.labelPosition),m(),P("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),$("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),ee("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),m(9),$("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),m(),k(r.hideIcon?-1:11),m(),$("for",r.buttonId),ee("id",r._labelId)}},dependencies:[ur,nm],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),oI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[_v,se]})}return t})();var i2=["*"];var r2=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],o2=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],a2=new y("MAT_CARD_CONFIG"),Ha=(()=>{class t{appearance;constructor(){let e=d(a2,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&P("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:i2,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})(),aI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var za=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var Ua=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:o2,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(me(r2),z(0),rt(1,"div",0),z(2,1),pt(),z(3,2))},encapsulation:2,changeDetection:0})}return t})();var sI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var s2=["input"],l2=["formField"],c2=["*"],bv=class{source;value;constructor(n,e){this.source=n,this.value=e}};var d2=new y("MatRadioGroup"),u2=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})});var f2=(()=>{class t{_elementRef=d(L);_changeDetector=d(ye);_focusMonitor=d(Ft);_radioDispatcher=d(Q_);_defaultOptions=d(u2,{optional:!0});_ngZone=d(G);_renderer=d(je);_uniqueId=d(Ve).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new X;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=Oe();_injector=d(j);constructor(){d(lt).load(ni);let e=d(d2,{optional:!0}),i=d(new un("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=Ut(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new bv(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_onTouchTargetClick(e){this._onInputInteraction(e),(!this.disabled||this.disabledInteractive)&&this._inputElement?.nativeElement.focus()}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,Be(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===r&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&Re(s2,5)(l2,7,L),i&2){let o;q(o=Q())&&(r._inputElement=o.first),q(o=Q())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&W("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(ee("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),P("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",te],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Ut(e)],checked:[2,"checked","checked",te],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",te],required:[2,"required","required",te],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",te]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:c2,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition"],[1,"mdc-radio"],["aria-hidden","true",1,"mat-mdc-radio-touch-target",3,"click"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mdc-label",3,"for"]],template:function(i,r){i&1&&(me(),h(0,"div",2,0)(2,"div",3)(3,"div",4),W("click",function(a){return r._onTouchTargetClick(a)}),p(),h(4,"input",5,1),W("change",function(a){return r._onInputInteraction(a)}),p(),h(6,"div",6),re(7,"div",7)(8,"div",8),p(),h(9,"div",9),re(10,"div",10),p()(),h(11,"label",11),z(12),p()()),i&2&&($("labelPosition",r.labelPosition),m(2),P("mdc-radio--disabled",r.disabled),m(2),$("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ee("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),m(5),$("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0),m(2),$("for",r.inputId))},dependencies:[ur,nm],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-hover-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-pressed-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button label {
  cursor: pointer;
}
.mat-mdc-radio-button label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--mat-radio-checked-ripple-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--mat-radio-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-radio-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-radio-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-radio-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-radio-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-radio-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-radio-button .mdc-radio--disabled + label {
  color: var(--mat-radio-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-radio-touch-target-size, 48px);
  width: var(--mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),lI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[ii,f2,se]})}return t})();var m2=["mat-menu-item",""],h2=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],p2=["mat-icon, [matMenuItemIcon]","*"];function g2(t,n){t&1&&(qt(),h(0,"svg",2),re(1,"polygon",3),p())}var _2=["*"];function v2(t,n){if(t&1){let e=$e();rt(0,"div",0),Ko("click",function(){_e(e);let r=D();return ve(r.closed.emit("click"))})("animationstart",function(r){_e(e);let o=D();return ve(o._onAnimationStart(r.animationName))})("animationend",function(r){_e(e);let o=D();return ve(o._onAnimationDone(r.animationName))})("animationcancel",function(r){_e(e);let o=D();return ve(o._onAnimationDone(r.animationName))}),rt(1,"div",1),z(2),pt()()}if(t&2){let e=D();st(e._classList),P("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Mt("id",e.panelId),ee("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var wv=new y("MAT_MENU_PANEL"),tc=(()=>{class t{_elementRef=d(L);_document=d(K);_focusMonitor=d(Ft);_parentMenu=d(wv,{optional:!0});_changeDetectorRef=d(ye);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new x;_focused=new x;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(lt).load(ni),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&W("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(ee("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),P("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",te],disableRipple:[2,"disableRipple","disableRipple",te]},exportAs:["matMenuItem"],attrs:m2,ngContentSelectors:p2,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(me(h2),z(0),h(1,"span",0),z(2,1),p(),re(3,"div",1),T(4,g2,2,0,":svg:svg",2)),i&2&&(m(3),$("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),m(),k(r._triggersSubmenu?4:-1))},dependencies:[ur],encapsulation:2,changeDetection:0})}return t})();var b2=new y("MatMenuContent");var y2=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),yv="_mat-menu-enter",rm="_mat-menu-exit",Ga=(()=>{class t{_elementRef=d(L);_changeDetectorRef=d(ye);_injector=d(j);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Oe();_allItems;_directDescendantItems=new Tn;_classList={};_panelAnimationState="void";_animationDone=new x;_isAnimating=F(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=b({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new X;close=this.closed;panelId=d(Ve).getId("mat-menu-panel-");constructor(){let e=d(y2);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Pl(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(Ge(this._directDescendantItems),Ee(e=>Bt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(Ge(this._directDescendantItems),Ee(i=>Bt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:ct(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=Be(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=J(b({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===rm;(i||e===yv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===yv||e===rm)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(rm),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?yv:rm)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(Ge(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,b2,5)(o,tc,5)(o,tc,4),i&2){let a;q(a=Q())&&(r.lazyContent=a.first),q(a=Q())&&(r._allItems=a),q(a=Q())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&Re(mt,5),i&2){let o;q(o=Q())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&ee("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",te],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:te(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Ae([{provide:wv,useExisting:t}])],ngContentSelectors:_2,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(me(),Ws(0,v2,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),w2=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Ii(t)}});var $a=new WeakMap,C2=(()=>{class t{_canHaveBackdrop;_element=d(L);_viewContainerRef=d(ht);_menuItemInstance=d(tc,{optional:!0,self:!0});_dir=d(Ze,{optional:!0});_focusMonitor=d(Ft);_ngZone=d(G);_injector=d(j);_scrollStrategy=d(w2);_changeDetectorRef=d(ye);_animationsDisabled=Oe();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=be.EMPTY;_menuCloseSubscription=be.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(wv,{optional:!0});this._parentMaterialMenu=i instanceof Ga?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&$a.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=$a.get(i);$a.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Ga&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(fe(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Ga&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ce(1)).subscribe(()=>{i.detach(),$a.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&$a.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=jn(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Ga&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Ln({positionStrategy:go(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[u,f]=[r,o],g=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}g=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:g},{originX:o,originY:l,overlayX:f,overlayY:a,offsetY:g},{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:-g},{originX:o,originY:c,overlayX:f,overlayY:s,offsetY:-g}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Z(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(ue(a=>this._menuOpen&&a!==this._menuItemInstance)):Z();return Bt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Pn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return $a.get(e)===this}_triggerIsAriaDisabled(){return te(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){ru()};static \u0275dir=H({type:t})}return t})(),cI=(()=>{class t extends C2{_cleanupTouchstart;_hoverSubscription=be.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new X;onMenuOpen=this.menuOpened;menuClosed=new X;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(je);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{uo(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){co(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&W("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&ee("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[De]})}return t})();var dI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[ii,pn,se,mn]})}return t})();var om=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new Cv(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},Cv=class{row;col;constructor(n,e){this.row=n,this.col=e}};var uI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-line",""],["","matLine",""]],hostAttrs:[1,"mat-line"]})}return t})();function fI(t,n,e="mat"){t.changes.pipe(Ge(t)).subscribe(({length:i})=>{nc(n,`${e}-2-line`,!1),nc(n,`${e}-3-line`,!1),nc(n,`${e}-multi-line`,!1),i===2||i===3?nc(n,`${e}-${i}-line`,!0):i>3&&nc(n,`${e}-multi-line`,!0)})}function nc(t,n,e){t.nativeElement.classList.toggle(n,e)}var Dv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[se]})}return t})();var mI=["*"],x2=[[["","mat-grid-avatar",""],["","matGridAvatar",""]],[["","mat-line",""],["","matLine",""]],"*"],E2=["[mat-grid-avatar], [matGridAvatar]","[mat-line], [matLine]","*"],I2=`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`,hI=new y("MAT_GRID_LIST"),rc=(()=>{class t{_element=d(L);_gridList=d(hI,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round($t(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round($t(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&ee("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:mI,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(me(),rt(0,"div",0),z(1),pt())},styles:[`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),am=(()=>{class t{_element=d(L);_lines;constructor(){}ngAfterContentInit(){fI(this._lines,this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-grid-tile-header"],["mat-grid-tile-footer"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,uI,5),i&2){let a;q(a=Q())&&(r._lines=a)}},ngContentSelectors:E2,decls:4,vars:0,consts:[[1,"mat-grid-list-text"]],template:function(i,r){i&1&&(me(x2),z(0),rt(1,"div",0),z(2,1),pt(),z(3,2))},encapsulation:2,changeDetection:0})}return t})();var sm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["mat-grid-tile-header"]],hostAttrs:[1,"mat-grid-tile-header"]})}return t})();var S2=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,ic=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=pI(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":bo(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),a=this._direction==="rtl"?"right":"left";n._setStyle(a,this.getTilePosition(o,e)),n._setStyle("width",bo(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},xv=class extends ic{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=pI(this.fixedRowHeight),S2.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",bo(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",bo(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},Ev=class extends ic{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",bo(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",bo(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},Iv=class extends ic{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",bo(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function bo(t){return`calc(${t})`}function pI(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var M2="fit",lm=(()=>{class t{_element=d(L);_dir=d(Ze,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round($t(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===M2?this._tileStyler=new Iv:e&&e.indexOf(":")>-1?this._tileStyler=new Ev(e):this._tileStyler=new xv(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new om);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,a)=>{let s=e.positions[a];this._tileStyler.setStyle(o,s.row,s.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&Ke(o,rc,5),i&2){let a;q(a=Q())&&(r._tiles=a)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&ee("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[Ae([{provide:hI,useExisting:t}])],ngContentSelectors:mI,decls:2,vars:0,template:function(i,r){i&1&&(me(),rt(0,"div"),z(1),pt())},styles:[I2],encapsulation:2,changeDetection:0})}return t})(),gI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[Dv,se,Dv]})}return t})();var vI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({})}return t})();var bI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[vI,se]})}return t})();function T2(t,n){}var _r=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Mv=(()=>{class t extends sr{_elementRef=d(L);_focusTrapFactory=d(Ml);_config;_interactivityChecker=d(Ca);_ngZone=d(G);_focusMonitor=d(Ft);_renderer=d(je);_changeDetectorRef=d(ye);_injector=d(j);_platform=d(we);_document=d(K);_portalOutlet;_focusTrapped=new x;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(_r,{optional:!0})||new _r,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Be(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=El(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=El();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=El()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Re(lr,7),i&2){let o;q(o=Q())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&ee("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[De],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&He(0,T2,0,0,"ng-template",0)},dependencies:[lr],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),oc=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new x;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!ct(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},k2=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Sa(t)}}),R2=new y("DialogData"),A2=new y("DefaultDialogConfig");function O2(t){let n=F(t),e=new X;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var Tv=(()=>{class t{_injector=d(j);_defaultOptions=d(A2,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(wf);_idGenerator=d(Ve);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;_ariaHiddenElements=new Map;_scrollStrategy=d(k2);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=bn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ge(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new _r;i=b(b({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=jn(this._injector,o),s=new oc(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Ce(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){Sv(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Sv(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Sv(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Ln({positionStrategy:e.positionStrategy||dr().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:_r,useValue:r},{provide:oc,useValue:i},{provide:Ea,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=Mv;let l=new Fn(s,r.viewContainerRef,j.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof mt){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=b(b({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Pn(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new Fn(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:R2,useValue:e.data},{provide:oc,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Ze,null,{optional:!0}))&&s.push({provide:Ze,useValue:O2(e.direction)}),j.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Sv(t,n){let e=t.length;for(;e--;)n(t[e])}var yI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({providers:[Tv],imports:[pn,cr,kl,cr]})}return t})();function N2(t,n){}var dm=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},kv="mdc-dialog--open",wI="mdc-dialog--opening",CI="mdc-dialog--closing",F2=150,P2=75,L2=(()=>{class t extends Mv{_animationStateChanged=new X;_animationsEnabled=!Oe();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?xI(this._config.enterAnimationDuration)??F2:0;_exitAnimationDuration=this._animationsEnabled?xI(this._config.exitAnimationDuration)??P2:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(DI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(wI,kv)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(kv),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(kv),this._animationsEnabled?(this._hostElement.style.setProperty(DI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(CI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(wI,CI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Mt("id",r._config.id),ee("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),P("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[De],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(h(0,"div",0)(1,"div",1),He(2,N2,0,0,"ng-template",2),p()())},dependencies:[lr],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),DI="--mat-dialog-transition-duration";function xI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?$t(t.substring(0,t.length-2)):t.endsWith("s")?$t(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var cm=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(cm||{}),yo=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new vn(1);_beforeClosed=new vn(1);_result;_closeFallbackTimeout;_state=cm.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ue(r=>r.state==="opened"),Ce(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ue(r=>r.state==="closed"),Ce(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Bt(this.backdropClick(),this.keydownEvents().pipe(ue(r=>r.keyCode===27&&!this.disableClose&&!ct(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),EI(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(ue(i=>i.state==="closing"),Ce(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=cm.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=cm.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function EI(t,n,e){return t._closeInteractionType=n,t.close(e)}var B2=new y("MatMdcDialogData"),j2=new y("mat-mdc-dialog-default-options"),V2=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(j);return()=>Sa(t)}}),ac=(()=>{class t{_defaultOptions=d(j2,{optional:!0});_scrollStrategy=d(V2);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ve);_injector=d(j);_dialog=d(Tv);_animationsDisabled=Oe();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;dialogConfigClass=dm;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=bn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ge(void 0)));constructor(){this._dialogRefConstructor=yo,this._dialogContainerType=L2,this._dialogDataToken=B2}open(e,i){let r;i=b(b({},this._defaultOptions||new dm),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,J(b({},i),{positionStrategy:dr(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:_r,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),II=(()=>{class t{dialogRef=d(yo,{optional:!0});_elementRef=d(L);_dialog=d(ac);ariaLabel;type="button";dialogResult;_matDialogClose;constructor(){}ngOnInit(){this.dialogRef||(this.dialogRef=RI(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose||e._matDialogCloseResult;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){EI(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&W("click",function(a){return r._onButtonClick(a)}),i&2&&ee("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[qe]})}return t})(),SI=(()=>{class t{_dialogRef=d(yo,{optional:!0});_elementRef=d(L);_dialog=d(ac);constructor(){}ngOnInit(){this._dialogRef||(this._dialogRef=RI(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t})}return t})(),MI=(()=>{class t extends SI{id=d(Ve).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&Mt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[De]})}return t})(),TI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=H({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[hg([Ei])]})}return t})(),kI=(()=>{class t extends SI{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Qe(t)))(r||t)}})();static \u0275dir=H({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&P("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[De]})}return t})();function RI(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var AI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=M({providers:[ac],imports:[yI,pn,cr,se]})}return t})();var Kt=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=R({type:t});static \u0275inj=M({imports:[Zx,UE,$E,WE,qE,tI,iI,vo,oI,sI,lI,Qf,dI,gI,bI,tm,AI]})};var vr=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-page-footer"]],decls:7,vars:3,consts:[[1,"page-footer","text-xs","flex","items-center","flex-row","flex-wrap"],["color","primary",1,"page-footer"],[1,"flex","flex-1","justify-end","text-xl"],["href","https://boardgamegeek.com/user/Vortilion","target","_blank",1,"text-blue-700"]],template:function(e,i){e&1&&(h(0,"div",0)(1,"mat-toolbar",1)(2,"div",2),v(3),vi(4,"transloco"),h(5,"a",3),v(6,"Vortilion"),p()()()()),e&2&&(m(3),I(" ",bi(4,1,"creator-prefix"),"\xA0"))},dependencies:[Kt,Yf,Oa],styles:[".page-footer[_ngcontent-%COMP%]{color:var(--mat-sys-primary);background:var(--bg-color)}"]})};function H2(t,n){if(t&1){let e=$e();h(0,"button",3),W("click",function(){let r=_e(e).$implicit,o=D();return ve(o.changeLanguage(r))}),h(1,"mat-icon"),v(2," language "),p(),h(3,"span"),v(4),vi(5,"transloco"),p()()}if(t&2){let e=n.$implicit,i=D();P("active",i.activeLang()===e),m(),Tt("color",i.activeLang()===e?"rgb(255 143 0)":null),m(2),Tt("color",i.activeLang()===e?"rgb(255 143 0)":null),m(),I(" ",bi(5,7,e+"-language-label")," ")}}var Wa=class t{translocoService=d(Aa);activeLang=F("");availableLangs=F([]);constructor(){let n=Yx(),e=this.translocoService.getAvailableLangs();if(this.availableLangs.set(e),n&&this.translocoService.isLang(n))this.activeLang.set(n),this.translocoService.getActiveLang()!==n&&Be(()=>{this.translocoService.setActiveLang(n)});else{let i=this.translocoService.getDefaultLang();this.activeLang.set(i)}}changeLanguage(n){typeof n=="string"&&(this.translocoService.setActiveLang(n),this.activeLang.set(this.translocoService.getActiveLang()))}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-language-selector"]],decls:10,vars:4,consts:[["menu","matMenu"],["mat-icon-button","","aria-label","Language Selection",1,"example-icon","favorite-icon",3,"matMenuTriggerFor"],["mat-menu-item","",3,"active"],["mat-menu-item","",3,"click"]],template:function(e,i){if(e&1&&(h(0,"button",1)(1,"mat-icon"),v(2,"translate"),p()(),h(3,"span"),v(4),vi(5,"transloco"),p(),h(6,"mat-menu",null,0),It(8,H2,6,9,"button",2,dn),p()),e&2){let r=Ct(7);$("matMenuTriggerFor",r),m(4),U(bi(5,2,i.activeLang()+"-language-label")),m(4),St(i.availableLangs())}},dependencies:[Kt,em,_o,Ga,tc,cI,Oa],encapsulation:2})};function z2(t,n){if(t&1){let e=$e();h(0,"button",7),W("click",function(){_e(e);let r=D();return ve(r.sidebarHandleRef.toggle())}),h(1,"mat-icon"),v(2,"menu"),p()()}if(t&2){let e=D();$("hidden",!e.isXSmall())}}function U2(t,n){if(t&1&&(h(0,"a",3)(1,"mat-icon"),v(2,"apps"),p()()),t&2){let e=D();$("routerLink",e.dashboardRouteRef)}}function $2(t,n){if(t&1&&(h(0,"span"),v(1),vi(2,"transloco"),p()),t&2){let e=D();m(),U(bi(2,1,e.titlePrefixRef+"app-title"))}}function G2(t,n){if(t&1&&(h(0,"span",5),v(1),vi(2,"transloco"),p()),t&2){let e=D();m(),U(bi(2,1,e.titlePrefixRef+"app-title-short"))}}var qa=class t{responsive=d(On);sidebarHandle=Zo();dashboardRoute=Zo();titlePrefix=Zo("");get sidebarHandleRef(){return this.sidebarHandle()}get dashboardRouteRef(){return this.dashboardRoute()}get titlePrefixRef(){return this.titlePrefix()}isXSmall=Si(this.responsive.observe(ar.XSmall).pipe(N(n=>n.matches)),{initialValue:!1});static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-page-header"]],inputs:{sidebarHandle:[1,"sidebarHandle"],dashboardRoute:[1,"dashboardRoute"],titlePrefix:[1,"titlePrefix"]},decls:9,vars:5,consts:[[1,"page-header"],[1,"main-toolbar","toolbar"],["type","button","mat-icon-button","",3,"hidden"],["mat-icon-button","","aria-label","Randomizers dashboard",3,"routerLink"],[1,"app-name","text-xl"],[1,"text-xl"],[1,"spacer"],["type","button","mat-icon-button","",3,"click","hidden"]],template:function(e,i){e&1&&(h(0,"div",0)(1,"mat-toolbar",1),T(2,z2,3,1,"button",2),T(3,U2,3,1,"a",3),h(4,"span",4),T(5,$2,3,3,"span")(6,G2,3,3,"span",5),p(),re(7,"span",6)(8,"app-language-selector"),p()()),e&2&&(m(),P("is-mobile",i.isXSmall()),m(),k(i.sidebarHandleRef?2:-1),m(),k(i.dashboardRouteRef?3:-1),m(2),k(i.isXSmall()?6:5))},dependencies:[Kt,Yf,em,_o,Wa,nf,Oa],styles:[".is-mobile[_ngcontent-%COMP%]{position:fixed;z-index:2}.app-name[_ngcontent-%COMP%]{margin:0 0 0 8px}.spacer[_ngcontent-%COMP%]{flex:1 1 auto}.mat-toolbar[_ngcontent-%COMP%]{color:var(--mat-sys-primary);background:var(--bg-color)}"]})};var Qa=class t{get(n){if(!this.isStorageAvailable())return null;let e=this.getString(n);if(e===null)return null;try{return JSON.parse(e)}catch{return e}}set(n,e){if(!this.isStorageAvailable())return!1;try{let i=typeof e=="string"?e:JSON.stringify(e);return globalThis.localStorage.setItem(n,i),!0}catch{return!1}}delete(n){if(!this.isStorageAvailable())return!1;try{return globalThis.localStorage.removeItem(n),!0}catch{return!1}}clear(){if(!this.isStorageAvailable())return!1;try{return globalThis.localStorage.clear(),!0}catch{return!1}}getNumber(n){let e=this.getString(n);if(e===null)return null;let i=Number(e);return Number.isFinite(i)?i:null}setNumber(n,e){this.setString(n,String(e))}getString(n){return this.isStorageAvailable()?globalThis.localStorage.getItem(n):null}setString(n,e){this.isStorageAvailable()&&globalThis.localStorage.setItem(n,e)}isStorageAvailable(){return typeof globalThis>"u"||!("localStorage"in globalThis)?!1:globalThis.localStorage!==void 0&&globalThis.localStorage!==null}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var W2=t=>({"w-full":t});function q2(t,n){if(t&1&&(h(0,"mat-option",10),v(1),h(2,"span"),v(3),p()()),t&2){let e=n.$implicit,i=D().$implicit;$("value",e.value),m(),I(" ",e.label," "),m(2),U(i("players-label"))}}function Q2(t,n){if(t&1&&(h(0,"li",21)(1,"span"),v(2),p()()),t&2){let e=n.$implicit;m(2),U(e.title)}}function Y2(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"ul",20),It(7,Q2,3,1,"li",21,dn),p()()),t&2){let e=D().$implicit,i=D();m(4),U(e("neutral-buildings-label")),m(3),St(i.randomNeutralBuildings())}}function K2(t,n){if(t&1&&(h(0,"div"),re(1,"img",22),p()),t&2){let e=n.$implicit;m(),$("src",e.sides[0].image,Qo)}}function Z2(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"div",20),It(7,K2,2,1,"div",null,dn),p()()),t&2){let e=D().$implicit,i=D();m(4),U(e("station-masters-label")),m(3),St(i.randomStationMasters())}}function X2(t,n){if(t&1&&(h(0,"span",17)(1,"span",23),v(2),p()()),t&2){let e=n.$implicit;m(2),to("",e.title,"",e.sides[0].title," ")}}function J2(t,n){if(t&1&&(h(0,"span",17)(1,"span",23),v(2),p()()),t&2){let e=n.$implicit;m(2),to("",e.title,": ",e.sides[0].title," ")}}function eB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step41")," ")}}function tB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step42")," ")}}function nB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step43")," ")}}function iB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step71")," ")}}function rB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step72")," ")}}function oB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step73")," ")}}function aB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step83")," ")}}function sB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step84")," ")}}function lB(t,n){if(t&1){let e=$e();yt(0),h(1,"div",2),re(2,"app-page-header",3),h(3,"mat-sidenav-container",4)(4,"mat-sidenav",5,0)(6,"div",6)(7,"h2"),v(8),p(),re(9,"mat-divider",7),h(10,"h3"),v(11),p(),h(12,"mat-form-field",8)(13,"mat-label"),v(14),p(),h(15,"mat-select",9),W("selectionChange",function(r){_e(e);let o=D();return ve(o.onPlayerCountChange(r))}),It(16,q2,4,3,"mat-option",10,dn),p()()()(),h(18,"mat-sidenav-content")(19,"div",11)(20,"div",12)(21,"div",13)(22,"button",14),W("click",function(){_e(e);let r=D();return ve(r.randomizeSetup())}),h(23,"span"),v(24),p()()(),h(25,"mat-grid-list",15),T(26,Y2,9,1,"mat-grid-tile"),T(27,Z2,9,1,"mat-grid-tile"),h(28,"mat-grid-tile")(29,"mat-grid-tile-header")(30,"h3")(31,"span"),v(32),p(),v(33,": "),p()(),h(34,"div",16),It(35,X2,3,2,"span",17,dn),p()(),h(37,"mat-grid-tile")(38,"mat-grid-tile-header")(39,"h3")(40,"span"),v(41),p(),v(42,": "),p()(),h(43,"div",16),It(44,J2,3,2,"span",17,dn),p()()(),h(46,"mat-card")(47,"mat-card-header")(48,"h3")(49,"span"),v(50),p()()(),h(51,"mat-card-content")(52,"ol",18)(53,"li"),v(54),h(55,"ul",19)(56,"li"),v(57),p(),h(58,"li"),v(59),p(),h(60,"li"),v(61),p()()(),h(62,"li"),v(63),h(64,"ul",19)(65,"li"),v(66),p(),h(67,"li"),v(68),p(),h(69,"li"),v(70),p()()(),h(71,"li"),v(72),h(73,"ul",19)(74,"li"),v(75),p(),h(76,"li"),v(77),p(),h(78,"li"),v(79),p()()(),h(80,"li"),v(81),h(82,"ul",19),T(83,eB,2,1,"li"),T(84,tB,2,1,"li"),T(85,nB,2,1,"li"),h(86,"li"),v(87),p()()(),h(88,"li"),v(89),p(),h(90,"li")(91,"p"),v(92),p(),h(93,"p"),v(94),p(),h(95,"p"),v(96),p()(),h(97,"li"),v(98),h(99,"ul",19),T(100,iB,2,1,"li"),T(101,rB,2,1,"li"),T(102,oB,2,1,"li"),p()(),h(103,"li"),v(104),h(105,"ul",19)(106,"li"),v(107),p(),h(108,"li"),v(109),p(),T(110,aB,2,1,"li"),T(111,sB,2,1,"li"),p()()()()()()()()(),re(112,"app-page-footer"),p(),wt()}if(t&2){let e=n.$implicit,i=Ct(5),r=D();m(),P("is-mobile",r.isXSmall()),m(),$("sidebarHandle",i)("dashboardRoute","/")("titlePrefix","argentina."),m(),Tt("padding-top",r.isXSmall()?56:0,"px"),m(),$("mode",r.isXSmall()?"over":"side")("fixedInViewport",r.isXSmall())("opened",!r.isXSmall()),m(4),I("",e("options-label"),":"),m(3),I("",e("player-count-label"),":"),m(3),U(e("player-count-select-label")),m(),$("value",r.playerCount()),m(),St(r.playerCountList()),m(6),st(Ks(53,W2,r.isXSmall())),m(2),U(e("btn-setup-label")),m(),$("cols",r.isMax1280()?1:2),m(),k(r.randomNeutralBuildings().length>0?26:-1),m(),k(r.randomStationMasters().length>0?27:-1),m(5),U(e("player-buildings-label")),m(3),St(r.randomPlayerBuildings()),m(6),U(e("cities-label")),m(3),St(r.randomCities()),m(6),U(e("further-setup-steps-label")),m(4),I(" ",e("further-steps-step1")," "),m(3),I(" ",e("further-steps-step1a")," "),m(2),I(" ",e("further-steps-step1b")," "),m(2),I(" ",e("further-steps-step-1c")," "),m(2),I(" ",e("further-steps-step2")," "),m(3),I(" ",e("further-steps-step21")," "),m(2),I(" ",e("further-steps-step22")," "),m(2),I(" ",e("further-steps-step23")," "),m(2),I(" ",e("further-steps-step3")," "),m(3),I(" ",e("further-steps-step31")," "),m(2),I(" ",e("further-steps-step32")," "),m(2),I(" ",e("further-steps-step33")," "),m(2),I(" ",e("further-steps-step4")," "),m(2),k(r.playerCount()===2?83:-1),m(),k(r.playerCount()===3?84:-1),m(),k(r.playerCount()===4?85:-1),m(2),I(" ",e("further-steps-step4b")," "),m(2),U(e("further-steps-step5")),m(3),U(e("further-steps-step6")),m(2),U(e("further-steps-step6a")),m(2),U(e("further-steps-step6b")),m(2),I(" ",e("further-steps-step7")," "),m(2),k(r.playerCount()===2?100:-1),m(),k(r.playerCount()===3?101:-1),m(),k(r.playerCount()===4?102:-1),m(2),I(" ",e("further-steps-step8")," "),m(3),U(e("further-steps-step81")),m(2),U(e("further-steps-step82")),m(),k(r.playerCount()>=3?110:-1),m(),k(r.playerCount()>3?111:-1)}}var um=class t{applicationConfigService=d(Tf);responsive=d(On);storageService=d(Qa);randomNeutralBuildings=F([]);randomPlayerBuildings=F([]);randomStationMasters=F([]);randomCities=F([]);playerCountList=F([{label:"2",value:2},{label:"3",value:3},{label:"4",value:4}]);isXSmall=Si(this.responsive.observe(ar.XSmall).pipe(N(n=>n.matches)),{initialValue:!1});isMax1280=Si(this.responsive.observe("(max-width: 1280px)").pipe(N(n=>n.matches)),{initialValue:!1});playerCount=this.applicationConfigService.playerCount;constructor(){let n=this.storageService.getNumber("rar-playerCount");n!==null?this.applicationConfigService.setPlayerCount(n):this.storageService.setNumber("rar-playerCount",2),this.randomizeSetup()}onPlayerCountChange(n){let e=Number(n.value);this.storageService.setNumber("rar-playerCount",e),this.applicationConfigService.setPlayerCount(e)}randomizeSetup(){this.randomNeutralBuildings.set(this.applicationConfigService.getRandomNeutralBuildingOrder()),this.randomStationMasters.set(this.applicationConfigService.getRandomStationMasters()),this.randomPlayerBuildings.set(this.applicationConfigService.getRandomPlayerBuildings()),this.randomCities.set(this.applicationConfigService.getRandomCities())}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-gwt-argentina"]],decls:1,vars:1,consts:[["sidenav",""],[4,"transloco","translocoRead"],[1,"argentina-component","flex","flex-col"],[3,"sidebarHandle","dashboardRoute","titlePrefix"],[1,"sidenav-container","flex-1"],["fixedTopGap","56",1,"sidenav",3,"mode","fixedInViewport","opened"],[1,"sidenav__inner","p-4"],[1,"divider","mb-4"],["appearance","fill"],[3,"selectionChange","value"],[3,"value"],[1,"sidenav-content","flex","flex-col","min-h-full"],[1,"flex-1","px-2","xSmall:px-10","py-5"],[1,"mb-4"],["mat-flat-button","","color","primary",3,"click"],["rowHeight","180px","gutterSize","5px",3,"cols"],[1,"flex","flex-wrap"],[1,"px-2","text-2xl","xSmall:text-xl","flex-1"],[1,"pl-4","list-decimal"],[1,"list-disc","pl-8","text-gray-400"],[1,"flex"],[1,"px-2","text-2xl","xSmall:text-xl"],["alt","Station Master","width","100",1,"px-1","xSmall:px-2","md:px-4",3,"src"],[1,"whitespace-nowrap"]],template:function(e,i){e&1&&He(0,lB,113,55,"ng-container",1),e&2&&$("translocoRead","argentina")},dependencies:[Kt,Jl,Xf,Va,ec,fr,Ha,za,Ua,Mi,pr,ja,ri,lm,rc,am,sm,qa,vr,hr],styles:[`.is-mobile[_ngcontent-%COMP%]   .sidenav-container[_ngcontent-%COMP%]{flex-shrink:0;flex-grow:1;flex-basis:auto}.divider[_ngcontent-%COMP%]{margin-bottom:16px!important}
`]})};function cB(t,n){if(t&1){let e=$e();yt(0),h(1,"div",1)(2,"header",2)(3,"div",3)(4,"div")(5,"p",4),v(6),p(),h(7,"h1",5),v(8),p(),h(9,"p",6),v(10),p()(),re(11,"app-language-selector"),p()(),h(12,"main",7)(13,"div",8)(14,"mat-card",9),W("click",function(){_e(e);let r=D();return ve(r.navigate("/gwt-argentina"))})("keydown.enter",function(){_e(e);let r=D();return ve(r.navigate("/gwt-argentina"))})("keydown.space",function(r){return _e(e),D().navigate("/gwt-argentina"),ve(r.preventDefault())}),h(15,"mat-card-header")(16,"mat-card-title"),v(17),p()(),h(18,"mat-card-content")(19,"p"),v(20),p()()(),h(21,"mat-card",9),W("click",function(){_e(e);let r=D();return ve(r.navigate("/gwt-2nd-edition"))})("keydown.enter",function(){_e(e);let r=D();return ve(r.navigate("/gwt-2nd-edition"))})("keydown.space",function(r){return _e(e),D().navigate("/gwt-2nd-edition"),ve(r.preventDefault())}),h(22,"mat-card-header")(23,"mat-card-title"),v(24),p()(),h(25,"mat-card-content")(26,"p"),v(27),p()()()()(),re(28,"app-page-footer"),p(),wt()}if(t&2){let e=n.$implicit;m(6),U(e("eyebrow")),m(2),U(e("title")),m(2),I(" ",e("subtitle")," "),m(7),U(e("argentina.title")),m(3),U(e("argentina.description")),m(4),U(e("second-edition.title")),m(3),U(e("second-edition.description"))}}var fm=class t{router=d(rr);navigate(n){this.router.navigate([n])}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-randomizers-dashboard"]],decls:1,vars:1,consts:[[4,"transloco","translocoRead"],[1,"dashboard","flex","min-h-screen","flex-col"],[1,"dashboard__hero","px-4","py-6","xSmall:px-8","xSmall:py-8"],[1,"dashboard__topbar","mb-8","flex","items-start","justify-between","gap-4"],[1,"dashboard__eyebrow","mb-2"],[1,"dashboard__title","text-4xl","xSmall:text-5xl"],[1,"dashboard__subtitle","mt-3","max-w-3xl","text-base","xSmall:text-lg"],[1,"flex-1","px-4","pb-8","xSmall:px-8"],[1,"grid","gap-4","lg:grid-cols-2"],[1,"dashboard-card",3,"click","keydown.enter","keydown.space"]],template:function(e,i){e&1&&He(0,cB,29,7,"ng-container",0),e&2&&$("translocoRead","dashboard")},dependencies:[hr,Wa,Kt,Ha,za,Ua,aI,vr],styles:[".dashboard[_ngcontent-%COMP%]{background:radial-gradient(circle at top left,rgba(255,235,205,.85),transparent 38%),linear-gradient(180deg,#f6efe2,#ebe2cf 48%,#dfd3bc)}.dashboard__eyebrow[_ngcontent-%COMP%]{color:#8d6e63;font-size:.85rem;font-weight:700;letter-spacing:.16em;text-transform:uppercase}.dashboard__title[_ngcontent-%COMP%]{color:#3e2723;font-weight:700;line-height:1}.dashboard__subtitle[_ngcontent-%COMP%]{color:#5d4037}.dashboard-card[_ngcontent-%COMP%]{background:#fffcf7e0;border:1px solid rgba(109,76,65,.14);box-shadow:0 18px 40px #3e27231a;cursor:pointer;transition:box-shadow .2s ease,transform .2s ease}.dashboard-card[_ngcontent-%COMP%]:hover{box-shadow:0 24px 48px #3e27232e;transform:translateY(-2px)}"]})};var mm=class t{useVariant=F(null);useRailsToTheNorth=F(!1);playerCount=F(2);setUseVariant(n){this.useVariant.set(n)}setUseRailsToTheNorth(n){this.useRailsToTheNorth.set(n)}setPlayerCount(n){this.playerCount.set(n)}neutralBuildings=[{title:"A",sides:[{title:"front"}]},{title:"B",sides:[{title:"front"}]},{title:"C",sides:[{title:"front"}]},{title:"D",sides:[{title:"front"}]},{title:"E",sides:[{title:"front"}]},{title:"F",sides:[{title:"front"}]},{title:"G",sides:[{title:"front"}]}];playerBuildings=[{title:"1",sides:[{title:"a"},{title:"b"}]},{title:"2",sides:[{title:"a"},{title:"b"}]},{title:"3",sides:[{title:"a"},{title:"b"}]},{title:"4",sides:[{title:"a"},{title:"b"}]},{title:"5",sides:[{title:"a"},{title:"b"}]},{title:"6",sides:[{title:"a"},{title:"b"}]},{title:"7",sides:[{title:"a"},{title:"b"}]},{title:"8",sides:[{title:"a"},{title:"b"}]},{title:"9",sides:[{title:"a"},{title:"b"}]},{title:"10",sides:[{title:"a"},{title:"b"}]},{title:"11",sides:[{title:"a"},{title:"b"}]},{title:"12",sides:[{title:"a"},{title:"b"}]}];stationMasters=[{title:"1",sides:[{title:"front",image:"img/gwt-second-edition/station-master-01.png"}]},{title:"2",sides:[{title:"front",image:"img/gwt-second-edition/station-master-02.png"}]},{title:"3",sides:[{title:"front",image:"img/gwt-second-edition/station-master-03.png"}]},{title:"4",sides:[{title:"front",image:"img/gwt-second-edition/station-master-04.png"}]},{title:"5",sides:[{title:"front",image:"img/gwt-second-edition/station-master-05.png"}]},{title:"6",sides:[{title:"front",image:"img/gwt-second-edition/station-master-06.png"}]},{title:"7",sides:[{title:"front",image:"img/gwt-second-edition/station-master-07.png"}]},{title:"8",sides:[{title:"front",image:"img/gwt-second-edition/station-master-08.png"}]},{title:"9",sides:[{title:"front",image:"img/gwt-second-edition/station-master-09.png"}]}];getRandomNeutralBuildingOrder(){return this.shuffleArray(this.neutralBuildings)}getRandomStationMasters(){let n=[],e=this.shuffleArray(this.stationMasters);for(let i=0;i<5;i+=1)n.push(e.pop());return n}getRandomPlayerBuildings(){let n=JSON.parse(JSON.stringify(this.playerBuildings));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}shuffleArray(n){let e=n.slice();for(let i=e.length-1;i>0;i-=1){let r=Math.floor(Math.random()*(i+1)),o=e[i];e[i]=e[r],e[r]=o}return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};function dB(t,n){if(t&1&&(yt(0),h(1,"h2",1),v(2),p(),h(3,"mat-dialog-content"),v(4),p(),h(5,"mat-dialog-actions")(6,"button",2),v(7,"Ok"),p()(),wt()),t&2){let e=n.$implicit;m(2),I(" ",e("title")," "),m(2),I(" ",e("content")," ")}}var hm=class t{dialogRef=d(yo);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-variant-warning-dialog"]],decls:1,vars:1,consts:[[4,"transloco","translocoRead"],["mat-dialog-title",""],["mat-button","","mat-dialog-close",""]],template:function(e,i){e&1&&He(0,dB,8,2,"ng-container",0),e&2&&$("translocoRead","second-edition.modals.variant-warning")},dependencies:[vo,fr,kI,II,MI,TI,hr],encapsulation:2})};var uB=t=>({"w-full":t}),fB=(t,n)=>n.value;function mB(t,n){if(t&1&&(h(0,"mat-option",10),v(1),h(2,"span"),v(3),p()()),t&2){let e=n.$implicit,i=D().$implicit;$("value",e.value),m(),I(" ",e.label," "),m(2),U(i("players-label"))}}function hB(t,n){if(t&1&&(h(0,"li",26)(1,"span"),v(2),p()()),t&2){let e=n.$implicit;m(2),U(e.title)}}function pB(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"ul",25),It(7,hB,3,1,"li",26,Qs),p()()),t&2){let e=D().$implicit,i=D();m(4),U(e("neutral-buildings-label")),m(3),St(i.randomNeutralBuildings())}}function gB(t,n){if(t&1&&(h(0,"div"),re(1,"img",27),p()),t&2){let e=n.$implicit;m(),$("src",e.sides[0].image,Qo)}}function _B(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"div",25),It(7,gB,2,1,"div",null,Qs),p()()),t&2){let e=D().$implicit,i=D();m(4),U(e("station-masters-label")),m(3),St(i.randomStationMasters())}}function vB(t,n){if(t&1&&(h(0,"span",22)(1,"span",28),v(2),p()()),t&2){let e=n.$implicit;m(2),to("",e.title,"",e.sides[0].title," ")}}function bB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),U(e("further-steps-step4_simmental"))}}function yB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),U(e("further-steps-step4_brahman"))}}function wB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),U(e("further-steps-step4"))}}function CB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),U(e("further-steps-step41_simmental_brahman"))}}function DB(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;I(" ",e("further-steps-step41")," ")}}function xB(t,n){if(t&1&&(h(0,"li"),T(1,CB,2,1,"ng-container")(2,DB,1,1),p()),t&2){let e=D(2);m(),k(e.useSimmental()||e.useBrahman()?1:2)}}function EB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),U(e("further-steps-step42_simmental_brahman"))}}function IB(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;I(" ",e("further-steps-step42")," ")}}function SB(t,n){if(t&1&&(h(0,"li"),T(1,EB,2,1,"ng-container")(2,IB,1,1),p()),t&2){let e=D(2);m(),k(e.useSimmental()||e.useBrahman()?1:2)}}function MB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),U(e("further-steps-step43_simmental_brahman"))}}function TB(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;I(" ",e("further-steps-step43")," ")}}function kB(t,n){if(t&1&&(h(0,"li"),T(1,MB,2,1,"ng-container")(2,TB,1,1),p()),t&2){let e=D(2);m(),k(e.useSimmental()||e.useBrahman()?1:2)}}function RB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-simmental2")," ")}}function AB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-simmental3")," ")}}function OB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-brahman")," ")}}function NB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step5")," ")}}function FB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step63")," ")}}function PB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step64")," ")}}function LB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step7")," ")}}function BB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step8")," ")}}function jB(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),h(7,"li"),v(8),p()()()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step9")," "),m(3),U(e("further-steps-step91")),m(2),U(e("further-steps-step92")),m(2),U(e("further-steps-step93"))}}function VB(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),h(7,"li"),v(8),p(),h(9,"li"),v(10),p()()()),t&2){let e=D().$implicit;m(),I(" ",e("further-steps-step10")," "),m(3),U(e("further-steps-step101")),m(2),U(e("further-steps-step102")),m(2),U(e("further-steps-step103")),m(2),U(e("further-steps-step104"))}}function HB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D(2).$implicit;m(),I(" ",e("further-steps-step11_3")," ")}}function zB(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),T(7,HB,2,1,"li"),p()()),t&2){let e=D().$implicit,i=D();m(),I(" ",e("further-steps-step11")," "),m(3),U(e("further-steps-step11_1")),m(2),U(e("further-steps-step11_2")),m(),k(i.useSimmental()?7:-1)}}function UB(t,n){if(t&1){let e=$e();yt(0),h(1,"div",2),re(2,"app-page-header",3),h(3,"mat-sidenav-container",4)(4,"mat-sidenav",5,0)(6,"div",6)(7,"h2"),v(8),p(),re(9,"mat-divider",7),h(10,"h3"),v(11),p(),h(12,"mat-form-field",8)(13,"mat-label"),v(14),p(),h(15,"mat-select",9),W("selectionChange",function(r){_e(e);let o=D();return ve(o.onPlayerCountChange(r))}),It(16,mB,4,3,"mat-option",10,fB),p()(),h(18,"h3"),v(19),p(),h(20,"div",11)(21,"mat-slide-toggle",12),W("change",function(r){_e(e);let o=D();return ve(o.onVariantChange("useSimmental",r))}),v(22),p(),h(23,"mat-slide-toggle",13),W("change",function(r){_e(e);let o=D();return ve(o.onVariantChange("useBrahman",r))}),v(24),p()(),h(25,"h3"),v(26),p(),h(27,"div",14)(28,"mat-slide-toggle",15),W("change",function(r){_e(e);let o=D();return ve(o.onExpansionChange(r))}),v(29),p()()()(),h(30,"mat-sidenav-content")(31,"div",16)(32,"div",17)(33,"div",18)(34,"button",19),W("click",function(){_e(e);let r=D();return ve(r.randomizeSetup())}),h(35,"span"),v(36),p()()(),h(37,"mat-grid-list",20),T(38,pB,9,1,"mat-grid-tile"),T(39,_B,9,1,"mat-grid-tile"),h(40,"mat-grid-tile")(41,"mat-grid-tile-header")(42,"h3")(43,"span"),v(44),p(),v(45,": "),p()(),h(46,"div",21),It(47,vB,3,2,"span",22,Qs),p()()(),h(49,"mat-card")(50,"mat-card-header")(51,"h3")(52,"span"),v(53),p()()(),h(54,"mat-card-content")(55,"ol",23)(56,"li"),v(57),h(58,"ul",24)(59,"li"),v(60),p(),h(61,"li"),v(62),p()()(),h(63,"li"),v(64),h(65,"ul",24)(66,"li"),v(67),p(),h(68,"li"),v(69),p(),h(70,"li"),v(71),p(),h(72,"li"),v(73),p()()(),h(74,"li"),v(75),h(76,"ul",24)(77,"li"),v(78),p(),h(79,"li"),v(80),p(),h(81,"li"),v(82),p()()(),h(83,"li"),T(84,bB,2,1,"ng-container"),T(85,yB,2,1,"ng-container"),T(86,wB,2,1,"ng-container"),h(87,"ul",24),T(88,xB,3,1,"li"),T(89,SB,3,1,"li"),T(90,kB,3,1,"li"),T(91,RB,2,1,"li"),T(92,AB,2,1,"li"),T(93,OB,2,1,"li"),p()(),T(94,NB,2,1,"li"),h(95,"li"),v(96),h(97,"ul",24)(98,"li"),v(99),p(),h(100,"li"),v(101),p(),T(102,FB,2,1,"li"),T(103,PB,2,1,"li"),p()(),T(104,LB,2,1,"li"),T(105,BB,2,1,"li"),T(106,jB,9,4,"li"),T(107,VB,11,5,"li"),T(108,zB,8,4,"li"),p()()()()()()(),re(109,"app-page-footer"),p(),wt()}if(t&2){let e=n.$implicit,i=Ct(5),r=D();m(),P("is-mobile",r.isXSmall()),m(),$("sidebarHandle",i)("dashboardRoute","/")("titlePrefix","second-edition."),m(),Tt("padding-top",r.isXSmall()?56:0,"px"),m(),$("mode",r.isXSmall()?"over":"side")("fixedInViewport",r.isXSmall())("opened",!r.isXSmall()),m(4),I("",e("options-label"),":"),m(3),I("",e("player-count-label"),":"),m(3),U(e("player-count-select-label")),m(),$("value",r.playerCount()),m(),St(r.playerCountList()),m(3),I("",e("variant-label"),":"),m(2),$("checked",r.useSimmental()),m(),I(" ",e("variant-simmental")," "),m(),$("checked",r.useBrahman()),m(),I(" ",e("variant-brahman")," "),m(2),I("",e("expansion-label"),":"),m(2),$("checked",r.useRailsToTheNorth()),m(),I(" ",e("expansion-rails")," "),m(5),st(Ks(62,uB,r.isXSmall())),m(2),U(e("btn-setup-label")),m(),$("cols",r.isMax1280()?1:2),m(),k(r.randomNeutralBuildings().length>0?38:-1),m(),k(r.randomStationMasters().length>0?39:-1),m(5),U(e("player-buildings-label")),m(3),St(r.randomPlayerBuildings()),m(6),U(e("further-setup-steps-label")),m(4),I(" ",e("further-steps-step1")," "),m(3),I(" ",e("further-steps-step1a")," "),m(2),I(" ",e("further-steps-step1b")," "),m(2),I(" ",e("further-steps-step2")," "),m(3),I(" ",e("further-steps-step21")," "),m(2),I(" ",e("further-steps-step22")," "),m(2),I(" ",e("further-steps-step23")," "),m(2),I(" ",e("further-steps-step24")," "),m(2),I(" ",e("further-steps-step3")," "),m(3),I(" ",e("further-steps-step31")," "),m(2),I(" ",e("further-steps-step32")," "),m(2),I(" ",e("further-steps-step33")," "),m(2),k(r.useSimmental()&&!r.useBrahman()?84:-1),m(),k(r.useBrahman()&&!r.useSimmental()?85:-1),m(),k(r.useBrahman()||r.useSimmental()?-1:86),m(2),k(r.playerCount()===2?88:-1),m(),k(r.playerCount()===3?89:-1),m(),k(r.playerCount()===4?90:-1),m(),k(r.useSimmental()?91:-1),m(),k(r.useSimmental()?92:-1),m(),k(r.useBrahman()?93:-1),m(),k(r.useRailsToTheNorth()?-1:94),m(2),I(" ",e("further-steps-step6")," "),m(3),U(e("further-steps-step61")),m(2),U(e("further-steps-step62")),m(),k(r.playerCount()>=3?102:-1),m(),k(r.playerCount()>3?103:-1),m(),k(r.useRailsToTheNorth()?104:-1),m(),k(r.useRailsToTheNorth()?105:-1),m(),k(r.useRailsToTheNorth()?106:-1),m(),k(r.useRailsToTheNorth()?107:-1),m(),k(r.useRailsToTheNorth()?108:-1)}}var pm=class t{dialog=d(ac);applicationConfigService=d(mm);responsive=d(On);storage=d(Qa);randomNeutralBuildings=F([]);randomPlayerBuildings=F([]);randomStationMasters=F([]);playerCountList=F([{label:"2",value:2},{label:"3",value:3},{label:"4",value:4}]);isXSmall=Si(this.responsive.observe(ar.XSmall).pipe(N(n=>n.matches)),{initialValue:!1});isMax1280=Si(this.responsive.observe("(max-width: 1280px)").pipe(N(n=>n.matches)),{initialValue:!1});playerCount=this.applicationConfigService.playerCount;useSimmental=F(!1);useBrahman=F(!1);useRailsToTheNorth=this.applicationConfigService.useRailsToTheNorth;constructor(){let n=this.storage.get("gwt2-playerCount");typeof n=="number"?this.applicationConfigService.setPlayerCount(n):this.storage.set("gwt2-playerCount",2);let e=this.storage.get("gwt2-useSimmental");typeof e=="boolean"?(this.useSimmental.set(e),this.applicationConfigService.setUseVariant({name:"useSimmental",checked:e})):this.storage.set("gwt2-useSimmental",!1);let i=this.storage.get("gwt2-useBrahman");typeof i=="boolean"?(this.useBrahman.set(i),this.applicationConfigService.setUseVariant({name:"useBrahman",checked:i})):this.storage.set("gwt2-useBrahman",!1);let r=this.storage.get("gwt2-useRailsToTheNorth");typeof r=="boolean"?this.applicationConfigService.setUseRailsToTheNorth(r):this.storage.set("gwt2-useRailsToTheNorth",!1),this.randomizeSetup()}openDialog(){return this.dialog.open(hm)}onPlayerCountChange(n){let e=Number(n.value);this.storage.set("gwt2-playerCount",e),this.applicationConfigService.setPlayerCount(e)}resetVariants(){this.openDialog().afterClosed().subscribe(()=>{this.storage.set("gwt2-useSimmental",!1),this.useSimmental.set(!1),this.applicationConfigService.setUseVariant({name:"useSimmental",checked:!1}),this.storage.set("gwt2-useBrahman",!1),this.useBrahman.set(!1),this.applicationConfigService.setUseVariant({name:"useBrahman",checked:!1})})}onVariantChange(n,e){this.useBrahman()&&n==="useSimmental"&&e.checked||this.useSimmental()&&n==="useBrahman"&&e.checked?this.resetVariants():(this.storage.set(`gwt2-${e.source.name}`,e.checked),n==="useSimmental"?this.useSimmental.set(e.checked):n==="useBrahman"&&this.useBrahman.set(e.checked),this.applicationConfigService.setUseVariant({name:n,checked:e.checked}))}onExpansionChange(n){this.storage.set("gwt2-useRailsToTheNorth",n.checked),this.applicationConfigService.setUseRailsToTheNorth(n.checked)}randomizeSetup(){this.randomNeutralBuildings.set(this.applicationConfigService.getRandomNeutralBuildingOrder()),this.randomStationMasters.set(this.applicationConfigService.getRandomStationMasters()),this.randomPlayerBuildings.set(this.applicationConfigService.getRandomPlayerBuildings())}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-gwt-second-edition"]],decls:1,vars:1,consts:[["sidenav",""],[4,"transloco","translocoRead"],[1,"second-edition-component","flex","flex-col"],[3,"sidebarHandle","dashboardRoute","titlePrefix"],[1,"sidenav-container","flex-1"],["fixedTopGap","56",1,"sidenav",3,"mode","fixedInViewport","opened"],[1,"sidenav__inner","p-4"],[1,"divider","mb-4"],["appearance","fill"],[3,"selectionChange","value"],[3,"value"],[1,"options-list","space-y-2","mb-4"],["color","primary","name","useSimmental",1,"options-list__option",3,"change","checked"],["color","primary","name","useBrahman",1,"options-list__option",3,"change","checked"],[1,"options-list","space-y-2"],["color","primary","name","useRailsToTheNorth",1,"options-list__option",3,"change","checked"],[1,"sidenav-content","flex","flex-col","min-h-full"],[1,"flex-1","px-2","xSmall:px-10","py-5"],[1,"mb-4"],["mat-flat-button","","color","primary",3,"click"],["rowHeight","180px","gutterSize","5px",3,"cols"],[1,"flex","flex-wrap"],[1,"px-2","text-2xl","xSmall:text-xl","flex-1"],[1,"pl-4","list-decimal"],[1,"list-disc","pl-8","text-gray-400"],[1,"flex"],[1,"px-2","text-2xl","xSmall:text-xl"],["alt","Station Master",1,"px-1","xSmall:px-2","md:px-4",3,"src"],[1,"whitespace-nowrap"]],template:function(e,i){e&1&&He(0,UB,110,64,"ng-container",1),e&2&&$("translocoRead","second-edition")},dependencies:[OE,Kt,Jl,Xf,Va,ec,fr,_v,Ha,za,Ua,Mi,pr,ja,ri,lm,rc,am,sm,qa,vr,hr],styles:[`.is-mobile[_ngcontent-%COMP%]   .sidenav-container[_ngcontent-%COMP%]{flex-shrink:0;flex-grow:1;flex-basis:auto}.divider[_ngcontent-%COMP%]{margin-bottom:16px!important}.options-list[_ngcontent-%COMP%]{display:flex;flex-direction:column}
`]})};var OI=[{path:"",component:fm},{path:"gwt-argentina",component:um},{path:"gwt-2nd-edition",component:pm},{path:"**",redirectTo:"",pathMatch:"full"}];var NI={providers:[g_(OI),Ug($g()),Qx({config:{availableLangs:["de","en","pl"],defaultLang:"en",fallbackLang:"en",missingHandler:{useFallbackTranslation:!0},reRenderOnLangChange:!0,prodMode:!xg()},loader:Mf}),od(OD.register("ngsw-worker.js",{enabled:!xg(),registrationStrategy:"registerWhenStable:30000"}))]};Fg(Sf,NI).catch(t=>console.error(t));
