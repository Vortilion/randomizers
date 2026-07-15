var lS=Object.defineProperty,cS=Object.defineProperties;var dS=Object.getOwnPropertyDescriptors;var dc=Object.getOwnPropertySymbols;var qv=Object.prototype.hasOwnProperty,Qv=Object.prototype.propertyIsEnumerable;var Wv=(t,n,e)=>n in t?lS(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,b=(t,n)=>{for(var e in n||={})qv.call(n,e)&&Wv(t,e,n[e]);if(dc)for(var e of dc(n))Qv.call(n,e)&&Wv(t,e,n[e]);return t},X=(t,n)=>cS(t,dS(n));var wm=(t,n)=>{var e={};for(var i in t)qv.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&dc)for(var i of dc(t))n.indexOf(i)<0&&Qv.call(t,i)&&(e[i]=t[i]);return e};var kt=null,uc=!1,Cm=1,uS=null,Ze=Symbol("SIGNAL");function te(t){let n=kt;return kt=t,n}function fc(){return kt}var Oi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ni(t){if(uc)throw new Error("");if(kt===null)return;kt.consumerOnSignalRead(t);let n=kt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=kt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:kt.producers,e!==void 0&&e.producer===t)){kt.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===kt&&(!i||mS(r,kt)))return;let o=xo(kt),a={producer:t,consumer:kt,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};kt.producersTail=a,n!==void 0?n.nextProducer=a:kt.producers=a,o&&Xv(t,a)}function Yv(){Cm++}function wr(t){if(!(xo(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Cm)){if(!t.producerMustRecompute(t)&&!Do(t)){Co(t);return}t.producerRecomputeValue(t),Co(t)}}function Dm(t){if(t.consumers===void 0)return;let n=uc;uc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||fS(i)}}finally{uc=n}}function xm(){return kt?.consumerAllowSignalWrites!==!1}function fS(t){t.dirty=!0,Dm(t),t.consumerMarkedDirty?.(t)}function Co(t){t.dirty=!1,t.lastCleanEpoch=Cm}function si(t){return t&&Kv(t),te(t)}function Kv(t){t.producersTail=void 0,t.recomputing=!0}function Fi(t,n){te(n),t&&Zv(t)}function Zv(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(xo(t))do e=Em(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Do(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(wr(e),i!==e.version))return!0}return!1}function Pi(t){if(xo(t)){let n=t.producers;for(;n!==void 0;)n=Em(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Xv(t,n){let e=t.consumersTail,i=xo(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Xv(r.producer,r)}function Em(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!xo(n)){let o=n.producers;for(;o!==void 0;)o=Em(o)}return e}function xo(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function es(t){uS?.(t)}function mS(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function ts(t,n){return Object.is(t,n)}function ns(t,n){let e=Object.create(hS);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(wr(e),Ni(e),e.value===zn)throw e.error;return e.value};return i[Ze]=e,es(e),i}var br=Symbol("UNSET"),yr=Symbol("COMPUTING"),zn=Symbol("ERRORED"),hS=X(b({},Oi),{value:br,dirty:!0,error:null,equal:ts,kind:"computed",producerMustRecompute(t){return t.value===br||t.value===yr},producerRecomputeValue(t){if(t.value===yr)throw new Error("");let n=t.value;t.value=yr;let e=si(t),i,r=!1;try{i=t.computation(),te(null),r=n!==br&&n!==zn&&i!==zn&&t.equal(n,i)}catch(o){i=zn,t.error=o}finally{Fi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function pS(){throw new Error}var Jv=pS;function eb(t){Jv(t)}function Im(t){Jv=t}var gS=null;function Sm(t,n){let e=Object.create(is);e.value=t,n!==void 0&&(e.equal=n);let i=()=>tb(e);return i[Ze]=e,es(e),[i,a=>Cr(e,a),a=>mc(e,a)]}function tb(t){return Ni(t),t.value}function Cr(t,n){xm()||eb(t),t.equal(t.value,n)||(t.value=n,_S(t))}function mc(t,n){xm()||eb(t),Cr(t,n(t.value))}var is=X(b({},Oi),{equal:ts,value:void 0,kind:"signal"});function _S(t){t.version++,Yv(),Dm(t),gS?.(t)}var Mm=X(b({},Oi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Tm(t){if(t.dirty=!1,t.version>0&&!Do(t))return;t.version++;let n=si(t);try{t.cleanup(),t.fn()}finally{Fi(t,n)}}function pe(t){return typeof t=="function"}function Eo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var hc=Eo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Dr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var be=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(pe(i))try{i()}catch(o){n=o instanceof hc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{nb(o)}catch(a){n=n??[],a instanceof hc?n=[...n,...a.errors]:n.push(a)}}if(n)throw new hc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)nb(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Dr(e,n)}remove(n){let{_finalizers:e}=this;e&&Dr(e,n),n instanceof t&&n._removeParent(this)}};be.EMPTY=(()=>{let t=new be;return t.closed=!0,t})();var km=be.EMPTY;function pc(t){return t instanceof be||t&&"closed"in t&&pe(t.remove)&&pe(t.add)&&pe(t.unsubscribe)}function nb(t){pe(t)?t():t.unsubscribe()}var bn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Io={setTimeout(t,n,...e){let{delegate:i}=Io;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Io;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function gc(t){Io.setTimeout(()=>{let{onUnhandledError:n}=bn;if(n)n(t);else throw t})}function xr(){}var ib=Rm("C",void 0,void 0);function rb(t){return Rm("E",void 0,t)}function ob(t){return Rm("N",t,void 0)}function Rm(t,n,e){return{kind:t,value:n,error:e}}var Er=null;function So(t){if(bn.useDeprecatedSynchronousErrorHandling){let n=!Er;if(n&&(Er={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Er;if(Er=null,e)throw i}}else t()}function ab(t){bn.useDeprecatedSynchronousErrorHandling&&Er&&(Er.errorThrown=!0,Er.error=t)}var Ir=class extends be{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,pc(n)&&n.add(this)):this.destination=yS}static create(n,e,i){return new li(n,e,i)}next(n){this.isStopped?Om(ob(n),this):this._next(n)}error(n){this.isStopped?Om(rb(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Om(ib,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},vS=Function.prototype.bind;function Am(t,n){return vS.call(t,n)}var Nm=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){_c(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){_c(i)}else _c(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){_c(e)}}},li=class extends Ir{constructor(n,e,i){super();let r;if(pe(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&bn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Am(n.next,o),error:n.error&&Am(n.error,o),complete:n.complete&&Am(n.complete,o)}):r=n}this.destination=new Nm(r)}};function _c(t){bn.useDeprecatedSynchronousErrorHandling?ab(t):gc(t)}function bS(t){throw t}function Om(t,n){let{onStoppedNotification:e}=bn;e&&Io.setTimeout(()=>e(t,n))}var yS={closed:!0,next:xr,error:bS,complete:xr};var Mo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Nt(t){return t}function Fm(...t){return Pm(t)}function Pm(t){return t.length===0?Nt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var re=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=CS(e)?e:new li(e,i,r);return So(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=sb(i),new i((r,o)=>{let a=new li({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Mo](){return this}pipe(...e){return Pm(e)(this)}toPromise(e){return e=sb(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function sb(t){var n;return(n=t??bn.Promise)!==null&&n!==void 0?n:Promise}function wS(t){return t&&pe(t.next)&&pe(t.error)&&pe(t.complete)}function CS(t){return t&&t instanceof Ir||wS(t)&&pc(t)}function DS(t){return pe(t?.lift)}function ce(t){return n=>{if(DS(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function de(t,n,e,i,r){return new Lm(t,n,e,i,r)}var Lm=class extends Ir{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var lb=Eo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var x=(()=>{class t extends re{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new vc(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new lb}next(e){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){So(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?km:(this.currentObservers=null,o.push(e),new be(()=>{this.currentObservers=null,Dr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new re;return e.source=this,e}}return t.create=(n,e)=>new vc(n,e),t})(),vc=class extends x{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:km}};var Xe=class extends x{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var rs={now(){return(rs.delegate||Date).now()},delegate:void 0};var yn=class extends x{constructor(n=1/0,e=1/0,i=rs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var bc=class extends be{constructor(n,e){super()}schedule(n,e=0){return this}};var os={setInterval(t,n,...e){let{delegate:i}=os;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=os;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var yc=class extends bc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return os.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&os.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Dr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var To=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};To.now=rs.now;var wc=class extends To{constructor(n,e=To.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var as=new wc(yc),cb=as;var Je=new re(t=>t.complete());function Cc(t){return t&&pe(t.schedule)}function Bm(t){return t[t.length-1]}function Dc(t){return pe(Bm(t))?t.pop():void 0}function Un(t){return Cc(Bm(t))?t.pop():void 0}function db(t,n){return typeof Bm(t)=="number"?t.pop():n}function fb(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(u){try{c(i.next(u))}catch(f){a(f)}}function l(u){try{c(i.throw(u))}catch(f){a(f)}}function c(u){u.done?o(u.value):r(u.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function ub(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Sr(t){return this instanceof Sr?(this.v=t,this):new Sr(t)}function mb(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(_){return function(C){return Promise.resolve(C).then(_,f)}}function s(_,C){i[_]&&(r[_]=function(O){return new Promise(function(L,Y){o.push([_,O,L,Y])>1||l(_,O)})},C&&(r[_]=C(r[_])))}function l(_,C){try{c(i[_](C))}catch(O){g(o[0][3],O)}}function c(_){_.value instanceof Sr?Promise.resolve(_.value.v).then(u,f):g(o[0][2],_)}function u(_){l("next",_)}function f(_){l("throw",_)}function g(_,C){_(C),o.shift(),o.length&&l(o[0][0],o[0][1])}}function hb(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof ub=="function"?ub(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var xc=t=>t&&typeof t.length=="number"&&typeof t!="function";function Ec(t){return pe(t?.then)}function Ic(t){return pe(t[Mo])}function Sc(t){return Symbol.asyncIterator&&pe(t?.[Symbol.asyncIterator])}function Mc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function xS(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Tc=xS();function kc(t){return pe(t?.[Tc])}function Rc(t){return mb(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Sr(e.read());if(r)return yield Sr(void 0);yield yield Sr(i)}}finally{e.releaseLock()}})}function Ac(t){return pe(t?.getReader)}function Pe(t){if(t instanceof re)return t;if(t!=null){if(Ic(t))return ES(t);if(xc(t))return IS(t);if(Ec(t))return SS(t);if(Sc(t))return pb(t);if(kc(t))return MS(t);if(Ac(t))return TS(t)}throw Mc(t)}function ES(t){return new re(n=>{let e=t[Mo]();if(pe(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function IS(t){return new re(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function SS(t){return new re(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,gc)})}function MS(t){return new re(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function pb(t){return new re(n=>{kS(t,n).catch(e=>n.error(e))})}function TS(t){return pb(Rc(t))}function kS(t,n){var e,i,r,o;return fb(this,void 0,void 0,function*(){try{for(e=hb(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Gt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Oc(t,n=0){return ce((e,i)=>{e.subscribe(de(i,r=>Gt(i,t,()=>i.next(r),n),()=>Gt(i,t,()=>i.complete(),n),r=>Gt(i,t,()=>i.error(r),n)))})}function Nc(t,n=0){return ce((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function gb(t,n){return Pe(t).pipe(Nc(n),Oc(n))}function _b(t,n){return Pe(t).pipe(Nc(n),Oc(n))}function vb(t,n){return new re(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function bb(t,n){return new re(e=>{let i;return Gt(e,n,()=>{i=t[Tc](),Gt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>pe(i?.return)&&i.return()})}function Fc(t,n){if(!t)throw new Error("Iterable cannot be null");return new re(e=>{Gt(e,n,()=>{let i=t[Symbol.asyncIterator]();Gt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function yb(t,n){return Fc(Rc(t),n)}function wb(t,n){if(t!=null){if(Ic(t))return gb(t,n);if(xc(t))return vb(t,n);if(Ec(t))return _b(t,n);if(Sc(t))return Fc(t,n);if(kc(t))return bb(t,n);if(Ac(t))return yb(t,n)}throw Mc(t)}function je(t,n){return n?wb(t,n):Pe(t)}function Z(...t){let n=Un(t);return je(t,n)}function ss(t,n){let e=pe(t)?t:()=>t,i=r=>r.error(e());return new re(n?r=>n.schedule(i,0,r):i)}function ls(t){return!!t&&(t instanceof re||pe(t.lift)&&pe(t.subscribe))}var Mr=Eo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Cb(t){return t instanceof Date&&!isNaN(t)}function U(t,n){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:RS}=Array;function AS(t,n){return RS(n)?t(...n):t(n)}function Pc(t){return U(n=>AS(t,n))}var{isArray:OS}=Array,{getPrototypeOf:NS,prototype:FS,keys:PS}=Object;function Lc(t){if(t.length===1){let n=t[0];if(OS(n))return{args:n,keys:null};if(LS(n)){let e=PS(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function LS(t){return t&&typeof t=="object"&&NS(t)===FS}function Bc(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Tr(...t){let n=Un(t),e=Dc(t),{args:i,keys:r}=Lc(t);if(i.length===0)return je([],n);let o=new re(BS(i,n,r?a=>Bc(r,a):Nt));return e?o.pipe(Pc(e)):o}function BS(t,n,e=Nt){return i=>{Db(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)Db(n,()=>{let c=je(t[l],n),u=!1;c.subscribe(de(i,f=>{o[l]=f,u||(u=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function Db(t,n,e){t?Gt(e,t,n):n()}function xb(t,n,e,i,r,o,a,s){let l=[],c=0,u=0,f=!1,g=()=>{f&&!l.length&&!c&&n.complete()},_=O=>c<i?C(O):l.push(O),C=O=>{o&&n.next(O),c++;let L=!1;Pe(e(O,u++)).subscribe(de(n,Y=>{r?.(Y),o?_(Y):n.next(Y)},()=>{L=!0},void 0,()=>{if(L)try{for(c--;l.length&&c<i;){let Y=l.shift();a?Gt(n,a,()=>C(Y)):C(Y)}g()}catch(Y){n.error(Y)}}))};return t.subscribe(de(n,_,()=>{f=!0,g()})),()=>{s?.()}}function Ft(t,n,e=1/0){return pe(n)?Ft((i,r)=>U((o,a)=>n(i,o,r,a))(Pe(t(i,r))),e):(typeof n=="number"&&(e=n),ce((i,r)=>xb(i,r,t,e)))}function jc(t=1/0){return Ft(Nt,t)}function Eb(){return jc(1)}function Li(...t){return Eb()(je(t,Un(t)))}function wn(t){return new re(n=>{Pe(t()).subscribe(n)})}function ci(...t){let n=Dc(t),{args:e,keys:i}=Lc(t),r=new re(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let u=0;u<a;u++){let f=!1;Pe(e[u]).subscribe(de(o,g=>{f||(f=!0,c--),s[u]=g},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Bc(i,s):s),o.complete())}))}});return n?r.pipe(Pc(n)):r}function Vc(t=0,n,e=cb){let i=-1;return n!=null&&(Cc(n)?e=n:i=n),new re(r=>{let o=Cb(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Pt(...t){let n=Un(t),e=db(t,1/0),i=t;return i.length?i.length===1?Pe(i[0]):jc(e)(je(i,n)):Je}var di=new re(xr);function ue(t,n){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Ib(t){return ce((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(de(e,c=>{i=!0,r=c,o||Pe(t(c)).subscribe(o=de(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Hc(t,n=as){return Ib(()=>Vc(t,n))}function $n(t){return ce((n,e)=>{let i=null,r=!1,o;i=n.subscribe(de(e,void 0,void 0,a=>{o=Pe(t(a,$n(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function ko(t,n){return pe(n)?Ft(t,n,1):Ft(t,1)}function kr(t,n=as){return ce((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}s()}e.subscribe(de(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function Sb(t){return ce((n,e)=>{let i=!1;n.subscribe(de(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ce(t){return t<=0?()=>Je:ce((n,e)=>{let i=0;n.subscribe(de(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function zc(t){return U(()=>t)}function Uc(t,n=Nt){return t=t??jS,ce((e,i)=>{let r,o=!0;e.subscribe(de(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function jS(t,n){return t===n}function Mb(t=VS){return ce((n,e)=>{let i=!1;n.subscribe(de(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function VS(){return new Mr}function Bi(t){return ce((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function ui(t,n){let e=arguments.length>=2;return i=>i.pipe(t?ue((r,o)=>t(r,o,i)):Nt,Ce(1),e?Sb(n):Mb(()=>new Mr))}function $c(t){return t<=0?()=>Je:ce((n,e)=>{let i=[];n.subscribe(de(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Gc(){return ce((t,n)=>{let e,i=!1;t.subscribe(de(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function jm(t=1/0){let n;t&&typeof t=="object"?n=t:n={count:t};let{count:e=1/0,delay:i,resetOnSuccess:r=!1}=n;return e<=0?Nt:ce((o,a)=>{let s=0,l,c=()=>{let u=!1;l=o.subscribe(de(a,f=>{r&&(s=0),a.next(f)},void 0,f=>{if(s++<e){let g=()=>{l?(l.unsubscribe(),l=null,c()):u=!0};if(i!=null){let _=typeof i=="number"?Vc(i):Pe(i(f,s)),C=de(a,()=>{C.unsubscribe(),g()},()=>{a.complete()});_.subscribe(C)}else g()}else a.error(f)})),u&&(l.unsubscribe(),l=null,c())};c()})}function cs(t={}){let{connector:n=()=>new x,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,u=!1,f=!1,g=()=>{s?.unsubscribe(),s=void 0},_=()=>{g(),a=l=void 0,u=f=!1},C=()=>{let O=a;_(),O?.unsubscribe()};return ce((O,L)=>{c++,!f&&!u&&g();let Y=l=l??n();L.add(()=>{c--,c===0&&!f&&!u&&(s=Vm(C,r))}),Y.subscribe(L),!a&&c>0&&(a=new li({next:Me=>Y.next(Me),error:Me=>{f=!0,g(),s=Vm(_,e,Me),Y.error(Me)},complete:()=>{u=!0,g(),s=Vm(_,i),Y.complete()}}),Pe(O).subscribe(a))})(o)}}function Vm(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new li({next:()=>{i.unsubscribe(),t()}});return Pe(n(...e)).subscribe(i)}function Rr(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,cs({connector:()=>new yn(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function ds(t){return ue((n,e)=>t<=e)}function Ge(...t){let n=Un(t);return ce((e,i)=>{(n?Li(t,e,n):Li(t,e)).subscribe(i)})}function Ie(t,n){return ce((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(de(i,l=>{r?.unsubscribe();let c=0,u=o++;Pe(t(l,u)).subscribe(r=de(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function fe(t){return ce((n,e)=>{Pe(t).subscribe(de(e,()=>e.complete(),xr)),!e.closed&&n.subscribe(e)})}function Hm(t,n=!1){return ce((e,i)=>{let r=0;e.subscribe(de(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function et(t,n,e){let i=pe(t)||n||e?{next:t,error:n,complete:e}:t;return i?ce((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(de(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Nt}var zm;function Wc(){return zm}function Gn(t){let n=zm;return zm=t,n}var Tb=Symbol("NotFound");function Ro(t){return t===Tb||t?.name==="\u0275NotFound"}function Um(t,n,e){let i=Object.create(HS);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(wr(i),Ni(i),i.value===zn)throw i.error;return i.value};return o[Ze]=i,es(i),o}function kb(t,n){wr(t),Cr(t,n),Co(t)}function Rb(t,n){if(wr(t),t.value===zn)throw t.error;mc(t,n),Co(t)}var HS=X(b({},Oi),{value:br,dirty:!0,error:null,equal:ts,kind:"linkedSignal",producerMustRecompute(t){return t.value===br||t.value===yr},producerRecomputeValue(t){if(t.value===yr)throw new Error("");let n=t.value;t.value=yr;let e=si(t),i,r=!1;try{let o=t.source(),a=n!==br&&n!==zn,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,te(null),r=a&&i!==zn&&t.equal(n,i)}catch(o){i=zn,t.error=o}finally{Fi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Ab(t){let n=te(null);try{return t()}finally{te(n)}}var Jc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",S=class extends Error{code;constructor(n,e){super(Cn(n,e)),this.code=n}};function zS(t){return`NG0${Math.abs(t)}`}function Cn(t,n){return`${zS(t)}${n?": "+n:""}`}var zi=globalThis;function Ae(t){for(let n in t)if(t[n]===Ae)return n;throw Error("")}function Lb(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function _s(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(_s).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function ed(t,n){return t?n?`${t} ${n}`:t:n||""}var US=Ae({__forward_ref__:Ae});function Zt(t){return t.__forward_ref__=Zt,t}function ct(t){return nh(t)?t():t}function nh(t){return typeof t=="function"&&t.hasOwnProperty(US)&&t.__forward_ref__===Zt}function w(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function M(t){return{providers:t.providers||[],imports:t.imports||[]}}function vs(t){return $S(t,td)}function ih(t){return vs(t)!==null}function $S(t,n){return t.hasOwnProperty(n)&&t[n]||null}function GS(t){let n=t?.[td]??null;return n||null}function Gm(t){return t&&t.hasOwnProperty(Qc)?t[Qc]:null}var td=Ae({\u0275prov:Ae}),Qc=Ae({\u0275inj:Ae}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function rh(t){return t&&!!t.\u0275providers}var bs=Ae({\u0275cmp:Ae}),ys=Ae({\u0275dir:Ae}),oh=Ae({\u0275pipe:Ae}),ah=Ae({\u0275mod:Ae}),fs=Ae({\u0275fac:Ae}),Pr=Ae({__NG_ELEMENT_ID__:Ae}),Ob=Ae({__NG_ENV_ID__:Ae});function sh(t){return id(t,"@NgModule"),t[ah]||null}function mi(t){return id(t,"@Component"),t[bs]||null}function nd(t){return id(t,"@Directive"),t[ys]||null}function Bb(t){return id(t,"@Pipe"),t[oh]||null}function id(t,n){if(t==null)throw new S(-919,!1)}function Lr(t){return typeof t=="string"?t:t==null?"":String(t)}var jb=Ae({ngErrorCode:Ae}),WS=Ae({ngErrorMessage:Ae}),qS=Ae({ngTokenPath:Ae});function lh(t,n){return Vb("",-200,n)}function rd(t,n){throw new S(-201,!1)}function Vb(t,n,e){let i=new S(n,t);return i[jb]=n,i[WS]=t,e&&(i[qS]=e),i}function QS(t){return t[jb]}var Wm;function Hb(){return Wm}function Lt(t){let n=Wm;return Wm=t,n}function ch(t,n,e){let i=vs(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;rd(t,"")}var YS={},Ar=YS,KS="__NG_DI_FLAG__",qm=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Or(e)||0;try{return this.injector.get(n,i&8?null:Ar,i)}catch(r){if(Ro(r))return r;throw r}}};function ZS(t,n=0){let e=Wc();if(e===void 0)throw new S(-203,!1);if(e===null)return ch(t,void 0,n);{let i=XS(n),r=e.retrieve(t,i);if(Ro(r)){if(i.optional)return null;throw r}return r}}function H(t,n=0){return(Hb()||ZS)(ct(t),n)}function d(t,n){return H(t,Or(n))}function Or(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function XS(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Qm(t){let n=[];for(let e=0;e<t.length;e++){let i=ct(t[e]);if(Array.isArray(i)){if(i.length===0)throw new S(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=JS(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(H(r,o))}else n.push(H(i))}return n}function JS(t){return t[KS]}function ji(t,n){let e=t.hasOwnProperty(fs);return e?t[fs]:null}function zb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Ub(t){return t.flat(Number.POSITIVE_INFINITY)}function od(t,n){t.forEach(e=>Array.isArray(e)?od(e,n):n(e))}function dh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function ws(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function $b(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Gb(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function ad(t,n,e){let i=Oo(t,n);return i>=0?t[i|1]=e:(i=~i,Gb(t,i,n,e)),i}function sd(t,n){let e=Oo(t,n);if(e>=0)return t[e|1]}function Oo(t,n){return eM(t,n,1)}function eM(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Dn={},Rt=[],Br=new y(""),uh=new y("",-1),fh=new y(""),ms=class{get(n,e=Ar){if(e===Ar){let r=Vb("",-201);throw r.name="\u0275NotFound",r}return e}};function xt(t){return{\u0275providers:t}}function ld(...t){return{\u0275providers:mh(!0,t),\u0275fromNgModule:!0}}function mh(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return od(n,a=>{let s=a;Yc(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&Wb(r,o),e}function Wb(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];hh(r,o=>{n(o,i)})}}function Yc(t,n,e,i){if(t=ct(t),!t)return!1;let r=null,o=Gm(t),a=!o&&mi(t);if(!o&&!a){let l=t.ngModule;if(o=Gm(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)Yc(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;od(o.imports,u=>{Yc(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&Wb(c,n)}if(!s){let c=ji(r)||(()=>new r);n({provide:r,useFactory:c,deps:Rt},r),n({provide:fh,useValue:r,multi:!0},r),n({provide:Br,useValue:()=>H(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;hh(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function hh(t,n){for(let e of t)rh(e)&&(e=e.\u0275providers),Array.isArray(e)?hh(e,n):n(e)}var tM=Ae({provide:String,useValue:Ae});function qb(t){return t!==null&&typeof t=="object"&&tM in t}function nM(t){return!!(t&&t.useExisting)}function iM(t){return!!(t&&t.useFactory)}function Nr(t){return typeof t=="function"}function Qb(t){return!!t.useClass}var Cs=new y(""),qc={},Nb={},$m;function No(){return $m===void 0&&($m=new ms),$m}var Le=class{},Fr=class extends Le{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Km(n,a=>this.processProvider(a)),this.records.set(uh,Ao(void 0,this)),r.has("environment")&&this.records.set(Le,Ao(void 0,this));let o=this.records.get(Cs);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(fh,Rt,{self:!0}))}retrieve(n,e){let i=Or(e)||0;try{return this.get(n,Ar,i)}catch(r){if(Ro(r))return r;throw r}}destroy(){us(this),this._destroyed=!0;let n=te(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),te(n)}}onDestroy(n){return us(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){us(this);let e=Gn(this),i=Lt(void 0),r;try{return n()}finally{Gn(e),Lt(i)}}get(n,e=Ar,i){if(us(this),n.hasOwnProperty(Ob))return n[Ob](this);let r=Or(i),o,a=Gn(this),s=Lt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=lM(n)&&vs(n);u&&this.injectableDefInScope(u)?c=Ao(Ym(n),qc):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?No():this.parent;return e=r&8&&e===Ar?null:e,l.get(n,e)}catch(l){let c=QS(l);throw c===-200||c===-201?new S(c,null):l}finally{Lt(s),Gn(a)}}resolveInjectorInitializers(){let n=te(null),e=Gn(this),i=Lt(void 0),r;try{let o=this.get(Br,Rt,{self:!0});for(let a of o)a()}finally{Gn(e),Lt(i),te(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=ct(n);let e=Nr(n)?n:ct(n&&n.provide),i=oM(n);if(!Nr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Ao(void 0,qc,!0),r.factory=()=>Qm(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=te(null);try{if(e.value===Nb)throw lh("");return e.value===qc&&(e.value=Nb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&sM(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{te(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=ct(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Ym(t){let n=vs(t),e=n!==null?n.factory:ji(t);if(e!==null)return e;if(t instanceof y)throw new S(-204,!1);if(t instanceof Function)return rM(t);throw new S(-204,!1)}function rM(t){if(t.length>0)throw new S(-204,!1);let e=GS(t);return e!==null?()=>e.factory(t):()=>new t}function oM(t){if(qb(t))return Ao(void 0,t.useValue);{let n=ph(t);return Ao(n,qc)}}function ph(t,n,e){let i;if(Nr(t)){let r=ct(t);return ji(r)||Ym(r)}else if(qb(t))i=()=>ct(t.useValue);else if(iM(t))i=()=>t.useFactory(...Qm(t.deps||[]));else if(nM(t))i=(r,o)=>H(ct(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=ct(t&&(t.useClass||t.provide));if(aM(t))i=()=>new r(...Qm(t.deps));else return ji(r)||Ym(r)}return i}function us(t){if(t.destroyed)throw new S(-205,!1)}function Ao(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function aM(t){return!!t.deps}function sM(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function lM(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Km(t,n){for(let e of t)Array.isArray(e)?Km(e,n):e&&rh(e)?Km(e.\u0275providers,n):n(e)}function ut(t,n){let e;t instanceof Fr?(us(t),e=t):e=new qm(t);let i,r=Gn(e),o=Lt(void 0);try{return n()}finally{Gn(r),Lt(o)}}function Yb(){return Hb()!==void 0||Wc()!=null}var xn=0,ne=1,se=2,dt=3,on=4,jt=5,jr=6,Fo=7,tt=8,hi=9,En=10,Be=11,Po=12,gh=13,Vr=14,Vt=15,Ui=16,Hr=17,qn=18,pi=19,_h=20,fi=21,cd=22,Vi=23,Xt=24,zr=25,$i=26,We=27,Kb=1,vh=6,Gi=7,Ds=8,Ur=9,Qe=10;function gi(t){return Array.isArray(t)&&typeof t[Kb]=="object"}function In(t){return Array.isArray(t)&&t[Kb]===!0}function bh(t){return(t.flags&4)!==0}function Qn(t){return t.componentOffset>-1}function Lo(t){return(t.flags&1)===1}function Sn(t){return!!t.template}function Bo(t){return(t[se]&512)!==0}function $r(t){return(t[se]&256)===256}var yh="svg",Zb="math";function an(t){for(;Array.isArray(t);)t=t[xn];return t}function wh(t,n){return an(n[t])}function sn(t,n){return an(n[t.index])}function dd(t,n){return t.data[n]}function Ch(t,n){return t[n]}function Dh(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function ln(t,n){let e=n[t];return gi(e)?e:e[xn]}function Xb(t){return(t[se]&4)===4}function ud(t){return(t[se]&128)===128}function Jb(t){return In(t[dt])}function Jt(t,n){return n==null?null:t[n]}function xh(t){t[Hr]=0}function Eh(t){t[se]&1024||(t[se]|=1024,ud(t)&&Gr(t))}function ey(t,n){for(;t>0;)n=n[Vr],t--;return n}function xs(t){return!!(t[se]&9216||t[Xt]?.dirty)}function fd(t){t[En].changeDetectionScheduler?.notify(8),t[se]&64&&(t[se]|=1024),xs(t)&&Gr(t)}function Gr(t){t[En].changeDetectionScheduler?.notify(0);let n=Hi(t);for(;n!==null&&!(n[se]&8192||(n[se]|=8192,!ud(n)));)n=Hi(n)}function Ih(t,n){if($r(t))throw new S(911,!1);t[fi]===null&&(t[fi]=[]),t[fi].push(n)}function ty(t,n){if(t[fi]===null)return;let e=t[fi].indexOf(n);e!==-1&&t[fi].splice(e,1)}function Hi(t){let n=t[dt];return In(n)?n[dt]:n}function Sh(t){return t[Fo]??=[]}function Mh(t){return t.cleanup??=[]}function ny(t,n,e,i){let r=Sh(n);r.push(e),t.firstCreatePass&&Mh(t).push(i,r.length-1)}var ge={lFrame:hy(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Zm=!1;function iy(){return ge.lFrame.elementDepthCount}function ry(){ge.lFrame.elementDepthCount++}function Th(){ge.lFrame.elementDepthCount--}function md(){return ge.bindingsEnabled}function kh(){return ge.skipHydrationRootTNode!==null}function Rh(t){return ge.skipHydrationRootTNode===t}function Ah(){ge.skipHydrationRootTNode=null}function oe(){return ge.lFrame.lView}function Ve(){return ge.lFrame.tView}function De(t){return ge.lFrame.contextLView=t,t[tt]}function xe(t){return ge.lFrame.contextLView=null,t}function ft(){let t=Oh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Oh(){return ge.lFrame.currentTNode}function oy(){let t=ge.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function jo(t,n){let e=ge.lFrame;e.currentTNode=t,e.isParent=n}function Nh(){return ge.lFrame.isParent}function Fh(){ge.lFrame.isParent=!1}function ay(){return ge.lFrame.contextLView}function Ph(){return Zm}function hs(t){let n=Zm;return Zm=t,n}function Lh(){let t=ge.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function sy(){return ge.lFrame.bindingIndex}function ly(t){return ge.lFrame.bindingIndex=t}function _i(){return ge.lFrame.bindingIndex++}function hd(t){let n=ge.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function cy(){return ge.lFrame.inI18n}function dy(t,n){let e=ge.lFrame;e.bindingIndex=e.bindingRootIndex=t,pd(n)}function uy(){return ge.lFrame.currentDirectiveIndex}function pd(t){ge.lFrame.currentDirectiveIndex=t}function fy(t){let n=ge.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function gd(){return ge.lFrame.currentQueryIndex}function Es(t){ge.lFrame.currentQueryIndex=t}function cM(t){let n=t[ne];return n.type===2?n.declTNode:n.type===1?t[jt]:null}function Bh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=cM(o),r===null||(o=o[Vr],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ge.lFrame=my();return i.currentTNode=n,i.lView=t,!0}function _d(t){let n=my(),e=t[ne];ge.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function my(){let t=ge.lFrame,n=t===null?null:t.child;return n===null?hy(t):n}function hy(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function py(){let t=ge.lFrame;return ge.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var jh=py;function vd(){let t=py();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function gy(t){return(ge.lFrame.contextLView=ey(t,ge.lFrame.contextLView))[tt]}function Yn(){return ge.lFrame.selectedIndex}function Wi(t){ge.lFrame.selectedIndex=t}function Vo(){let t=ge.lFrame;return dd(t.tView,t.selectedIndex)}function Wt(){ge.lFrame.currentNamespace=yh}function Is(){dM()}function dM(){ge.lFrame.currentNamespace=null}function Vh(){return ge.lFrame.currentNamespace}var _y=!0;function bd(){return _y}function Ss(t){_y=t}function Xm(t,n=null,e=null,i){let r=Hh(t,n,e,i);return r.resolveInjectorInitializers(),r}function Hh(t,n=null,e=null,i,r=new Set){let o=[e||Rt,ld(t)],a;return new Fr(o,n||No(),a||null,r)}var B=class t{static THROW_IF_NOT_FOUND=Ar;static NULL=new ms;static create(n,e){if(Array.isArray(n))return Xm({name:""},e,n,"");{let i=n.name??"";return Xm({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:t,providedIn:"any",factory:()=>H(uh)});static __NG_ELEMENT_ID__=-1},K=new y(""),mt=(()=>{class t{static __NG_ELEMENT_ID__=uM;static __NG_ENV_ID__=e=>e}return t})(),Kc=class extends mt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return $r(this._lView)}onDestroy(n){let e=this._lView;return Ih(e,n),()=>ty(e,n)}};function uM(){return new Kc(oe())}var vy=!1,by=new y(""),vi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Xe(!1);debugTaskTracker=d(by,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new re(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Jm=class extends x{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Yb()&&(this.destroyRef=d(mt,{optional:!0})??void 0,this.pendingTasks=d(vi,{optional:!0})??void 0)}emit(n){let e=te(null);try{super.next(n)}finally{te(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof be&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},$=Jm;function Zc(...t){}function zh(t){let n,e;function i(){t=Zc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function yy(t){return queueMicrotask(()=>t()),()=>{t=Zc}}var Uh="isAngularZone",ps=Uh+"_ID",fM=0,G=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new $(!1);onMicrotaskEmpty=new $(!1);onStable=new $(!1);onError=new $(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=vy}=n;if(typeof Zone>"u")throw new S(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,pM(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Uh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new S(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new S(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,mM,Zc,Zc);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},mM={};function $h(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function hM(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){zh(()=>{t.callbackScheduled=!1,eh(t),t.isCheckStableRunning=!0,$h(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),eh(t)}function pM(t){let n=()=>{hM(t)},e=fM++;t._inner=t._inner.fork({name:"angular",properties:{[Uh]:!0,[ps]:e,[ps+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(gM(l))return i.invokeTask(o,a,s,l);try{return Fb(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Pb(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return Fb(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!_M(l)&&n(),Pb(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,eh(t),$h(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function eh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Fb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Pb(t){t._nesting--,$h(t)}var gs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new $;onMicrotaskEmpty=new $;onStable=new $;onError=new $;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function gM(t){return wy(t,"__ignore_ng_zone__")}function _M(t){return wy(t,"__scheduler_tick__")}function wy(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Bt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},cn=new y("",{factory:()=>{let t=d(G),n=d(Le),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Bt),e.handleError(i))})}}}),Cy={provide:Br,useValue:()=>{let t=d(Bt,{optional:!0})},multi:!0};function le(t,n){let[e,i,r]=Sm(t,n?.equal),o=e,a=o[Ze];return o.set=i,o.update=r,o.asReadonly=yd.bind(o),o}function yd(){let t=this[Ze];if(t.readonlyFn===void 0){let n=()=>this();n[Ze]=t,t.readonlyFn=n}return t.readonlyFn}var Ho=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=vM}return t})();function vM(){return new Ho(oe(),ft())}var Wn=class{},Ms=new y("",{factory:()=>!0});var Gh=new y(""),Wr=(()=>{class t{internalPendingTasks=d(vi);scheduler=d(Wn);errorHandler=d(cn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),wd=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>new th})}return t})(),th=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Xc=class{[Ze];constructor(n){this[Ze]=n}destroy(){this[Ze].destroy()}};function qi(t,n){let e=n?.injector??d(B),i=n?.manualCleanup!==!0?e.get(mt):null,r,o=e.get(Ho,null,{optional:!0}),a=e.get(Wn);return o!==null?(r=wM(o.view,a,t),i instanceof Kc&&i._lView===o.view&&(i=null)):r=CM(t,e.get(wd),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Xc(r)}var Dy=X(b({},Mm),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=hs(!1);try{Tm(this)}finally{hs(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=te(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],te(t)}}}),bM=X(b({},Dy),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Pi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),yM=X(b({},Dy),{consumerMarkedDirty(){this.view[se]|=8192,Gr(this.view),this.notifier.notify(13)},destroy(){if(Pi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Vi]?.delete(this)}});function wM(t,n,e){let i=Object.create(yM);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=xy(i,e),t[Vi]??=new Set,t[Vi].add(i),i.consumerMarkedDirty(i),i}function CM(t,n,e){let i=Object.create(bM);return i.fn=xy(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function xy(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Bs(t){return{toString:t}.toString()}function TM(t){return typeof t=="function"}function s0(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Rd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Ue=(()=>{let t=()=>l0;return t.ngInherit=!0,t})();function l0(t){return t.type.prototype.ngOnChanges&&(t.setInput=RM),kM}function kM(){let t=d0(this),n=t?.current;if(n){let e=t.previous;if(e===Dn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function RM(t,n,e,i,r){let o=this.declaredInputs[i],a=d0(t)||AM(t,{previous:Dn,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Rd(c&&c.currentValue,e,l===Dn),s0(t,n,r,e)}var c0="__ngSimpleChanges__";function d0(t){return t[c0]||null}function AM(t,n){return t[c0]=n}var Ey=[];var Oe=function(t,n=null,e){for(let i=0;i<Ey.length;i++){let r=Ey[i];r(t,n,e)}},Ee=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Ee||{});function OM(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=l0(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function u0(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Id(t,n,e){f0(t,n,3,e)}function Sd(t,n,e,i){(t[se]&3)===e&&f0(t,n,e,i)}function Wh(t,n){let e=t[se];(e&3)===n&&(e&=16383,e+=1,t[se]=e)}function f0(t,n,e,i){let r=i!==void 0?t[Hr]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[Hr]+=65536),(s<o||o==-1)&&(NM(t,e,n,l),t[Hr]=(t[Hr]&4294901760)+l+2),l++}function Iy(t,n){Oe(Ee.LifecycleHookStart,t,n);let e=te(null);try{n.call(t)}finally{te(e),Oe(Ee.LifecycleHookEnd,t,n)}}function NM(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[se]>>14<t[Hr]>>16&&(t[se]&3)===n&&(t[se]+=16384,Iy(s,o)):Iy(s,o)}var Uo=-1,Qr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function FM(t){return(t.flags&8)!==0}function PM(t){return(t.flags&16)!==0}function LM(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];BM(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function m0(t){return t===3||t===4||t===6}function BM(t){return t.charCodeAt(0)===64}function $o(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?Sy(t,e,r,null,n[++i]):Sy(t,e,r,null,null))}}return t}function Sy(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function h0(t){return t!==Uo}function Ad(t){return t&32767}function jM(t){return t>>16}function Od(t,n){let e=jM(t),i=n;for(;e>0;)i=i[Vr],e--;return i}var np=!0;function Nd(t){let n=np;return np=t,n}var VM=256,p0=VM-1,g0=5,HM=0,Kn={};function zM(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Pr)&&(i=e[Pr]),i==null&&(i=e[Pr]=HM++);let r=i&p0,o=1<<r;n.data[t+(r>>g0)]|=o}function Fd(t,n){let e=_0(t,n);if(e!==-1)return e;let i=n[ne];i.firstCreatePass&&(t.injectorIndex=n.length,qh(i.data,t),qh(n,null),qh(i.blueprint,null));let r=Vp(t,n),o=t.injectorIndex;if(h0(r)){let a=Ad(r),s=Od(r,n),l=s[ne].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function qh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function _0(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Vp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=C0(r),i===null)return Uo;if(e++,r=r[Vr],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Uo}function ip(t,n,e){zM(t,n,e)}function UM(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(m0(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function v0(t,n,e){if(e&8||t!==void 0)return t;rd(n,"NodeInjector")}function b0(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[hi],o=Lt(void 0);try{return r?r.get(n,i,e&8):ch(n,i,e&8)}finally{Lt(o)}}return v0(i,n,e)}function y0(t,n,e,i=0,r){if(t!==null){if(n[se]&2048&&!(i&2)){let a=qM(t,n,e,i,Kn);if(a!==Kn)return a}let o=w0(t,n,e,i,Kn);if(o!==Kn)return o}return b0(n,e,i,r)}function w0(t,n,e,i,r){let o=GM(e);if(typeof o=="function"){if(!Bh(n,t,i))return i&1?v0(r,e,i):b0(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))rd(e);else return a}finally{jh()}}else if(typeof o=="number"){let a=null,s=_0(t,n),l=Uo,c=i&1?n[Vt][jt]:null;for((s===-1||i&4)&&(l=s===-1?Vp(t,n):n[s+8],l===Uo||!Ty(i,!1)?s=-1:(a=n[ne],s=Ad(l),n=Od(l,n)));s!==-1;){let u=n[ne];if(My(o,s,u.data)){let f=$M(s,n,e,a,i,c);if(f!==Kn)return f}l=n[s+8],l!==Uo&&Ty(i,n[ne].data[s+8]===c)&&My(o,s,n)?(a=u,s=Ad(l),n=Od(l,n)):s=-1}}return r}function $M(t,n,e,i,r,o){let a=n[ne],s=a.data[t+8],l=i==null?Qn(s)&&np:i!=a&&(s.type&3)!==0,c=r&1&&o===s,u=Md(s,a,e,l,c);return u!==null?As(n,a,u,s,r):Kn}function Md(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?s:s+u,g=r?s+u:c;for(let _=f;_<g;_++){let C=a[_];if(_<l&&e===C||_>=l&&C.type===e)return _}if(r){let _=a[l];if(_&&Sn(_)&&_.type===e)return l}return null}function As(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof Qr){let s=o;if(s.resolving)throw lh("");let l=Nd(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],u,f=s.injectImpl?Lt(s.injectImpl):null,g=Bh(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&OM(e,a[e],n)}finally{f!==null&&Lt(f),Nd(l),s.resolving=!1,jh()}}return o}function GM(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Pr)?t[Pr]:void 0;return typeof n=="number"?n>=0?n&p0:WM:n}function My(t,n,e){let i=1<<t;return!!(e[n+(t>>g0)]&i)}function Ty(t,n){return!(t&2)&&!(t&1&&n)}var qr=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return y0(this._tNode,this._lView,n,Or(i),e)}};function WM(){return new qr(ft(),oe())}function qe(t){return Bs(()=>{let n=t.prototype.constructor,e=n[fs]||rp(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[fs]||rp(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function rp(t){return nh(t)?()=>{let n=rp(ct(t));return n&&n()}:ji(t)}function qM(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[se]&2048&&!Bo(a);){let s=w0(o,a,e,i|2,Kn);if(s!==Kn)return s;let l=o.parent;if(!l){let c=a[_h];if(c){let u=c.get(e,Kn,i&-5);if(u!==Kn)return u}l=C0(a),a=a[Vr]}o=l}return r}function C0(t){let n=t[ne],e=n.type;return e===2?n.declTNode:e===1?t[jt]:null}function js(t){return UM(ft(),t)}function QM(){return Yo(ft(),oe())}function Yo(t,n){return new F(sn(t,n))}var F=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=QM}return t})();function D0(t){return t instanceof F?t.nativeElement:t}function YM(){return this._results[Symbol.iterator]()}var kn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new x}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Ub(n);(this._changesDetected=!zb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=YM};function x0(t){return(t.flags&128)===128}var Hp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Hp||{}),E0=new Map,KM=0;function ZM(){return KM++}function XM(t){E0.set(t[pi],t)}function op(t){E0.delete(t[pi])}var ky="__ngContext__";function Go(t,n){gi(n)?(t[ky]=n[pi],XM(n)):t[ky]=n}function I0(t){return M0(t[Po])}function S0(t){return M0(t[on])}function M0(t){for(;t!==null&&!In(t);)t=t[on];return t}var ap;function zp(t){ap=t}function T0(){if(ap!==void 0)return ap;if(typeof document<"u")return document;throw new S(210,!1)}var Yi=new y("",{factory:()=>JM}),JM="ng";var Qd=new y(""),Xr=new y("",{providedIn:"platform",factory:()=>"unknown"}),Vs=new y(""),Jr=new y("",{factory:()=>d(K).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var k0="r";var R0="di";var A0=!1,O0=new y("",{factory:()=>A0});var Ry=new WeakMap;function eT(t,n){if(t==null||typeof t!="object")return;let e=Ry.get(t);e||(e=new WeakSet,Ry.set(t,e)),e.add(n)}var tT=(t,n,e,i)=>{};function nT(t,n,e,i){tT(t,n,e,i)}function Yd(t){return(t.flags&32)===32}var iT=()=>null;function N0(t,n,e=!1){return iT(t,n,e)}function F0(t,n){let e=t.contentQueries;if(e!==null){let i=te(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];Es(o),s.contentQueries(2,n[a],a)}}}finally{te(i)}}}function sp(t,n,e){Es(0);let i=te(null);try{n(t,e)}finally{te(i)}}function Up(t,n,e){if(bh(n)){let i=te(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{te(i)}}}var Rn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Rn||{});var Cd;function rT(){if(Cd===void 0&&(Cd=null,zi.trustedTypes))try{Cd=zi.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Cd}function Kd(t){return rT()?.createHTML(t)||t}var Dd;function P0(){if(Dd===void 0&&(Dd=null,zi.trustedTypes))try{Dd=zi.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Dd}function Ay(t){return P0()?.createHTML(t)||t}function Oy(t){return P0()?.createScriptURL(t)||t}var bi=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Jc})`}},lp=class extends bi{getTypeName(){return"HTML"}},cp=class extends bi{getTypeName(){return"Style"}},dp=class extends bi{getTypeName(){return"Script"}},up=class extends bi{getTypeName(){return"URL"}},fp=class extends bi{getTypeName(){return"ResourceURL"}};function dn(t){return t instanceof bi?t.changingThisBreaksApplicationSecurity:t}function Xn(t,n){let e=L0(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Jc})`)}return e===n}function L0(t){return t instanceof bi&&t.getTypeName()||null}function $p(t){return new lp(t)}function Gp(t){return new cp(t)}function Wp(t){return new dp(t)}function qp(t){return new up(t)}function Qp(t){return new fp(t)}function oT(t){let n=new hp(t);return aT()?new mp(n):n}var mp=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Kd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},hp=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Kd(n),e}};function aT(){try{return!!new window.DOMParser().parseFromString(Kd(""),"text/html")}catch{return!1}}var sT=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Hs(t){return t=String(t),t.match(sT)?t:"unsafe:"+t}function yi(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function zs(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var B0=yi("area,br,col,hr,img,wbr"),j0=yi("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),V0=yi("rp,rt"),lT=zs(V0,j0),cT=zs(j0,yi("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),dT=zs(V0,yi("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Ny=zs(B0,cT,dT,lT),H0=yi("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),uT=yi("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),fT=yi("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),mT=zs(H0,uT,fT),hT=yi("script,style,template"),pp=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=_T(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=gT(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=Fy(n).toLowerCase();if(!Ny.hasOwnProperty(e))return this.sanitizedSomething=!0,!hT.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!mT.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;H0[s]&&(l=Hs(l)),this.buf.push(" ",a,'="',Py(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=Fy(n).toLowerCase();Ny.hasOwnProperty(e)&&!B0.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Py(n))}};function pT(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function gT(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw z0(n);return n}function _T(t){let n=t.firstChild;if(n&&pT(t,n))throw z0(n);return n}function Fy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function z0(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var vT=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,bT=/([^\#-~ |!])/g;function Py(t){return t.replace(/&/g,"&amp;").replace(vT,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(bT,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var xd;function Zd(t,n){let e=null;try{xd=xd||oT(t);let i=n?String(n):"";e=xd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=xd.getInertBodyElement(i)}while(i!==o);let s=new pp().sanitizeChildren(Ly(e)||e);return Kd(s)}finally{if(e){let i=Ly(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Ly(t){return"content"in t&&yT(t)?t.content:null}function yT(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var wT=/^>|^->|<!--|-->|--!>|<!-$/g,CT=/(<|>)/g,DT="\u200B$1\u200B";function xT(t){return t.replace(wT,n=>n.replace(CT,DT))}function ET(t,n){return t.createText(n)}function IT(t,n,e){t.setValue(n,e)}function ST(t,n){return t.createComment(xT(n))}function U0(t,n,e){return t.createElement(n,e)}function Pd(t,n,e,i,r){t.insertBefore(n,e,i,r)}function $0(t,n,e){t.appendChild(n,e)}function By(t,n,e,i,r){i!==null?Pd(t,n,e,i,r):$0(t,n,e)}function G0(t,n,e,i){t.removeChild(null,n,e,i)}function MT(t,n,e){t.setAttribute(n,"style",e)}function TT(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function W0(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&LM(t,n,i),r!==null&&TT(t,n,r),o!==null&&MT(t,n,o)}var ot=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(ot||{});function Yp(t){let n=Zp();return n?Ay(n.sanitize(ot.HTML,t)||""):Xn(t,"HTML")?Ay(dn(t)):Zd(T0(),Lr(t))}function Ko(t){let n=Zp();return n?n.sanitize(ot.URL,t)||"":Xn(t,"URL")?dn(t):Hs(Lr(t))}function q0(t){let n=Zp();if(n)return Oy(n.sanitize(ot.RESOURCE_URL,t)||"");if(Xn(t,"ResourceURL"))return Oy(dn(t));throw new S(904,!1)}var kT={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function RT(t,n){return kT[t.toLowerCase()]?.[n.toLowerCase()]===!0?q0:Ko}function Kp(t,n,e){return RT(n,e)(t)}function Zp(){let t=oe();return t&&t[En].sanitizer}function Q0(t){return t instanceof Function?t():t}function AT(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var Y0="ng-template";function OT(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&AT(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Xp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Xp(t){return t.type===4&&t.value!==Y0}function NT(t,n,e){let i=t.type===4&&!e?Y0:t.value;return n===i}function FT(t,n,e){let i=4,r=t.attrs,o=r!==null?BT(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!Mn(i)&&!Mn(l))return!1;if(a&&Mn(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!NT(t,l,e)||l===""&&n.length===1){if(Mn(i))return!1;a=!0}}else if(i&8){if(r===null||!OT(t,r,l,e)){if(Mn(i))return!1;a=!0}}else{let c=n[++s],u=PT(l,r,Xp(t),e);if(u===-1){if(Mn(i))return!1;a=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(Mn(i))return!1;a=!0}}}}return Mn(i)||a}function Mn(t){return(t&1)===0}function PT(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return jT(n,t)}function K0(t,n,e=!1){for(let i=0;i<n.length;i++)if(FT(t,n[i],e))return!0;return!1}function LT(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function BT(t){for(let n=0;n<t.length;n++){let e=t[n];if(m0(e))return n}return t.length}function jT(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function VT(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function jy(t,n){return t?":not("+n.trim()+")":n}function HT(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Mn(a)&&(n+=jy(o,r),r=""),i=a,o=o||!Mn(i);e++}return r!==""&&(n+=jy(o,r)),n}function zT(t){return t.map(HT).join(",")}function UT(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Mn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var qt={};function Jp(t,n,e,i,r,o,a,s,l,c,u){let f=We+i,g=f+r,_=$T(f,g),C=typeof c=="function"?c():c;return _[ne]={type:t,blueprint:_,template:e,queries:null,viewQuery:s,declTNode:n,data:_.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:C,incompleteFirstPass:!1,ssrId:u}}function $T(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:qt);return e}function GT(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Jp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function eg(t,n,e,i,r,o,a,s,l,c,u){let f=n.blueprint.slice();return f[xn]=r,f[se]=i|4|128|8|64|1024,(c!==null||t&&t[se]&2048)&&(f[se]|=2048),xh(f),f[dt]=f[Vr]=t,f[tt]=e,f[En]=a||t&&t[En],f[Be]=s||t&&t[Be],f[hi]=l||t&&t[hi]||null,f[jt]=o,f[pi]=ZM(),f[jr]=u,f[_h]=c,f[Vt]=n.type==2?t[Vt]:f,f}function WT(t,n,e){let i=sn(n,t),r=GT(e),o=t[En].rendererFactory,a=tg(t,eg(t,r,null,Z0(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function Z0(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function X0(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function tg(t,n){return t[Po]?t[gh][on]=n:t[Po]=n,t[gh]=n,n}function m(t=1){J0(Ve(),oe(),Yn()+t,!1)}function J0(t,n,e,i){if(!i)if((n[se]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Id(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Sd(n,o,0,e)}Wi(e)}var Xd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Xd||{});function gp(t,n,e,i){let r=te(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Xd.SignalBased)!==0&&(l=n[o][Ze]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):s0(n,l,o,i)}finally{te(r)}}var An=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(An||{}),qT;function ng(t,n){return qT(t,n)}var Q8=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var _p=new WeakMap,Ts=new WeakSet;function QT(t,n){let e=_p.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),Ts.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function YT(t,n){let e=_p.get(t);e?e.includes(n)||e.push(n):_p.set(t,[n])}var Yr=new Set,Jd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Jd||{}),On=new y(""),Vy=new Set;function Ki(t){Vy.has(t)||(Vy.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var eu=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),ig=[0,1,2,3],rg=(()=>{class t{ngZone=d(G);scheduler=d(Wn);errorHandler=d(Bt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(On,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Oe(Ee.AfterRenderHooksStart),this.executing=!0;for(let i of ig)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Oe(Ee.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[zr]??=[]).push(e),Gr(i),i[se]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Jd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Os=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[zr];n&&(this.view[zr]=n.filter(e=>e!==this))}};function $e(t,n){let e=n?.injector??d(B);return Ki("NgAfterNextRender"),ZT(t,e,n,!0)}function KT(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function ZT(t,n,e,i){let r=n.get(eu);r.impl??=n.get(rg);let o=n.get(On,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(mt):null,s=n.get(Ho,null,{optional:!0}),l=new Os(r.impl,KT(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var ew=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Le)})});function tw(t,n,e){let i=t.get(ew);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function XT(t,n){let e=t.get(ew);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function JT(t,n){for(let[e,i]of n)tw(t,i.animateFns)}function Hy(t,n,e,i){let r=t?.[$i]?.enter;n!==null&&r&&r.has(e.index)&&JT(i,r)}function zo(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;In(r)?l=r:gi(r)&&(c=!0,r=r[xn]);let u=an(r);t===0&&i!==null?(Hy(s,i,o,e),a==null?$0(n,i,u):Pd(n,i,u,a||null,!0)):t===1&&i!==null?(Hy(s,i,o,e),Pd(n,i,u,a||null,!0),QT(o,u)):t===2?(s?.[$i]?.leave?.has(o.index)&&YT(o,u),Ts.delete(u),zy(s,o,e,f=>{if(Ts.has(u)){Ts.delete(u);return}G0(n,u,c,f)})):t===3&&(Ts.delete(u),zy(s,o,e,()=>{n.destroyNode(u)})),l!=null&&dk(n,t,e,l,o,i,a)}}function ek(t,n){nw(t,n),n[xn]=null,n[jt]=null}function tk(t,n,e,i,r,o){i[xn]=r,i[jt]=n,nu(t,i,e,1,r,o)}function nw(t,n){n[En].changeDetectionScheduler?.notify(9),nu(t,n,n[Be],2,null,null)}function nk(t){let n=t[Po];if(!n)return Qh(t[ne],t);for(;n;){let e=null;if(gi(n))e=n[Po];else{let i=n[Qe];i&&(e=i)}if(!e){for(;n&&!n[on]&&n!==t;)gi(n)&&Qh(n[ne],n),n=n[dt];n===null&&(n=t),gi(n)&&Qh(n[ne],n),e=n&&n[on]}n=e}}function og(t,n){let e=t[Ur],i=e.indexOf(n);e.splice(i,1)}function tu(t,n){if($r(n))return;let e=n[Be];e.destroyNode&&nu(t,n,e,3,null,null),nk(n)}function Qh(t,n){if($r(n))return;let e=te(null);try{n[se]&=-129,n[se]|=256,n[Xt]&&Pi(n[Xt]),ok(t,n),rk(t,n),n[ne].type===1&&n[Be].destroy();let i=n[Ui];if(i!==null&&In(n[dt])){i!==n[dt]&&og(i,n);let r=n[qn];r!==null&&r.detachView(t)}op(n)}finally{te(e)}}function zy(t,n,e,i){let r=t?.[$i];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Yr.add(t[pi]),tw(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:u}=c();s.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),ik(t,i)}else t&&Yr.delete(t[pi]),i(!1)},r)}function ik(t,n){let e=t[$i]?.running;if(e){e.then(()=>{t[$i].running=void 0,Yr.delete(t[pi]),n(!0)});return}n(!1)}function rk(t,n){let e=t.cleanup,i=n[Fo];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[Fo]=null);let r=n[fi];if(r!==null){n[fi]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Vi];if(o!==null){n[Vi]=null;for(let a of o)a.destroy()}}function ok(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Qr)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Oe(Ee.LifecycleHookStart,s,l);try{l.call(s)}finally{Oe(Ee.LifecycleHookEnd,s,l)}}else{Oe(Ee.LifecycleHookStart,r,o);try{o.call(r)}finally{Oe(Ee.LifecycleHookEnd,r,o)}}}}}function iw(t,n,e){return ak(t,n.parent,e)}function ak(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[xn];if(Qn(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Rn.None||r===Rn.Emulated)return null}return sn(i,e)}function rw(t,n,e){return lk(t,n,e)}function sk(t,n,e){return t.type&40?sn(t,e):null}var lk=sk,Uy;function ag(t,n,e,i){let r=iw(t,i,n),o=n[Be],a=i.parent||n[jt],s=rw(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)By(o,r,e[l],s,!1);else By(o,r,e,s,!1);Uy!==void 0&&Uy(o,i,n,e,r)}function ks(t,n){if(n!==null){let e=n.type;if(e&3)return sn(n,t);if(e&4)return vp(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return ks(t,i);{let r=t[n.index];return In(r)?vp(-1,r):an(r)}}else{if(e&128)return ks(t,n.next);if(e&32)return ng(n,t)()||an(t[n.index]);{let i=ow(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Hi(t[Vt]);return ks(r,i)}else return ks(t,n.next)}}}return null}function ow(t,n){if(n!==null){let i=t[Vt][jt],r=n.projection;return i.projection[r]}return null}function vp(t,n){let e=Qe+t+1;if(e<n.length){let i=n[e],r=i[ne].firstChild;if(r!==null)return ks(i,r)}return n[Gi]}function sg(t,n,e,i,r,o,a){for(;e!=null;){let s=i[hi];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Go(an(l),i),e.flags|=2),!Yd(e))if(c&8)sg(t,n,e.child,i,r,o,!1),zo(n,t,s,r,l,e,o,i);else if(c&32){let u=ng(e,i),f;for(;f=u();)zo(n,t,s,r,f,e,o,i);zo(n,t,s,r,l,e,o,i)}else c&16?aw(t,n,i,e,r,o):zo(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function nu(t,n,e,i,r,o){sg(e,i,t.firstChild,n,r,o,!1)}function ck(t,n,e){let i=n[Be],r=iw(t,e,n),o=e.parent||n[jt],a=rw(o,e,n);aw(i,0,n,e,r,a)}function aw(t,n,e,i,r,o){let a=e[Vt],l=a[jt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];zo(n,t,e[hi],r,u,i,o,e)}else{let c=l,u=a[dt];x0(i)&&(c.flags|=128),sg(t,n,c,u,r,o,!0)}}function dk(t,n,e,i,r,o,a){let s=i[Gi],l=an(i);s!==l&&zo(n,t,e,o,s,r,a);for(let c=Qe;c<i.length;c++){let u=i[c];nu(u[ne],u,t,n,o,s)}}function uk(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:An.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=An.Important),t.setStyle(e,i,r,o))}}function sw(t,n,e,i,r){let o=Yn(),a=i&2;try{Wi(-1),a&&n.length>We&&J0(t,n,We,!1);let s=a?Ee.TemplateUpdateStart:Ee.TemplateCreateStart;Oe(s,r,e),e(i,r)}finally{Wi(o);let s=a?Ee.TemplateUpdateEnd:Ee.TemplateCreateEnd;Oe(s,r,e)}}function iu(t,n,e){gk(t,n,e),(e.flags&64)===64&&_k(t,n,e)}function Us(t,n,e=sn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function fk(t,n,e,i){let o=i.get(O0,A0)||e===Rn.ShadowDom||e===Rn.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return mk(a),a}function mk(t){hk(t)}var hk=()=>null;function pk(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function lw(t,n,e,i,r,o){let a=n[ne];if(ru(t,a,n,e,i)){Qn(t)&&dw(n,t.index);return}t.type&3&&(e=pk(e)),cw(t,n,e,i,r,o)}function cw(t,n,e,i,r,o){if(t.type&3){let a=sn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function dw(t,n){let e=ln(n,t);e[se]&16||(e[se]|=64)}function gk(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Qn(e)&&WT(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Fd(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=As(n,t,a,e);if(Go(l,n),o!==null&&yk(n,a-i,l,s,e,o),Sn(s)){let c=ln(e.index,n);c[tt]=As(n,t,a,e)}}}function _k(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=uy();try{Wi(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];pd(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&vk(l,c)}}finally{Wi(-1),pd(a)}}function vk(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function lg(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];K0(n,o.selectors,!1)&&(i??=[],Sn(o)?i.unshift(o):i.push(o))}return i}function bk(t,n,e,i,r,o){let a=sn(t,n);uw(n[Be],a,o,t.value,e,i,r)}function uw(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?Lr(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function yk(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];gp(i,e,l,c)}}function cg(t,n,e,i,r){let o=We+e,a=n[ne],s=r(a,n,t,i,e);n[o]=s,jo(t,!0);let l=t.type===2;return l?(W0(n[Be],s,t),(iy()===0||Lo(t))&&Go(s,n),ry()):Go(s,n),bd()&&(!l||!Yd(t))&&ag(a,n,s,t),t}function dg(t){let n=t;return Nh()?Fh():(n=n.parent,jo(n,!1)),n}function wk(t,n){let e=t[hi];if(!e)return;let i;try{i=e.get(cn,null)}catch{i=null}i?.(n)}function ru(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],u=a[l+1],f=n.data[c];gp(f,e[c],u,r),s=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];gp(u,c,i,r),s=!0}return s}function Ck(t,n){let e=ln(n,t),i=e[ne];Dk(i,e);let r=e[xn];r!==null&&e[jr]===null&&(e[jr]=N0(r,e[hi])),Oe(Ee.ComponentStart);try{ug(i,e,e[tt])}finally{Oe(Ee.ComponentEnd,e[tt])}}function Dk(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function ug(t,n,e){_d(n);try{let i=t.viewQuery;i!==null&&sp(1,i,e);let r=t.template;r!==null&&sw(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[qn]?.finishViewCreation(t),t.staticContentQueries&&F0(t,n),t.staticViewQueries&&sp(2,t.viewQuery,e);let o=t.components;o!==null&&xk(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[se]&=-5,vd()}}function xk(t,n){for(let e=0;e<n.length;e++)Ck(t,n[e])}function $s(t,n,e,i){let r=te(null);try{let o=n.tView,s=t[se]&4096?4096:16,l=eg(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Ui]=c;let u=t[qn];return u!==null&&(l[qn]=u.createEmbeddedView(o)),ug(o,l,e),l}finally{te(r)}}function Wo(t,n){return!n||n.firstChild===null||x0(t)}function Ns(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(an(o)),In(o)&&fw(o,i);let a=e.type;if(a&8)Ns(t,n,e.child,i);else if(a&32){let s=ng(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=ow(n,e);if(Array.isArray(s))i.push(...s);else{let l=Hi(n[Vt]);Ns(l[ne],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function fw(t,n){for(let e=Qe;e<t.length;e++){let i=t[e],r=i[ne].firstChild;r!==null&&Ns(i[ne],i,r,n)}t[Gi]!==t[xn]&&n.push(t[Gi])}function mw(t){if(t[zr]!==null){for(let n of t[zr])n.impl.addSequence(n);t[zr].length=0}}var hw=[];function Ek(t){return t[Xt]??Ik(t)}function Ik(t){let n=hw.pop()??Object.create(Mk);return n.lView=t,n}function Sk(t){t.lView[Xt]!==t&&(t.lView=null,hw.push(t))}var Mk=X(b({},Oi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Gr(t.lView)},consumerOnSignalRead(){this.lView[Xt]=this}});function Tk(t){let n=t[Xt]??Object.create(kk);return n.lView=t,n}var kk=X(b({},Oi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Hi(t.lView);for(;n&&!pw(n[ne]);)n=Hi(n);n&&Eh(n)},consumerOnSignalRead(){this.lView[Xt]=this}});function pw(t){return t.type!==2}function gw(t){if(t[Vi]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Vi])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[se]&8192)}}var Rk=100;function _w(t,n=0){let i=t[En].rendererFactory,r=!1;r||i.begin?.();try{Ak(t,n)}finally{r||i.end?.()}}function Ak(t,n){let e=Ph();try{hs(!0),bp(t,n);let i=0;for(;xs(t);){if(i===Rk)throw new S(103,!1);i++,bp(t,1)}}finally{hs(e)}}function Ok(t,n,e,i){if($r(n))return;let r=n[se],o=!1,a=!1;_d(n);let s=!0,l=null,c=null;o||(pw(t)?(c=Ek(n),l=si(c)):fc()===null?(s=!1,c=Tk(n),l=si(c)):n[Xt]&&(Pi(n[Xt]),n[Xt]=null));try{xh(n),ly(t.bindingStartIndex),e!==null&&sw(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&Id(n,_,null)}else{let _=t.preOrderHooks;_!==null&&Sd(n,_,0,null),Wh(n,0)}if(a||Nk(n),gw(n),vw(n,0),t.contentQueries!==null&&F0(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&Id(n,_)}else{let _=t.contentHooks;_!==null&&Sd(n,_,1),Wh(n,1)}Pk(t,n);let f=t.components;f!==null&&yw(n,f,0);let g=t.viewQuery;if(g!==null&&sp(2,g,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&Id(n,_)}else{let _=t.viewHooks;_!==null&&Sd(n,_,2),Wh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[cd]){for(let _ of n[cd])_();n[cd]=null}o||(mw(n),n[se]&=-73)}catch(u){throw o||Gr(n),u}finally{c!==null&&(Fi(c,l),s&&Sk(c)),vd()}}function vw(t,n){for(let e=I0(t);e!==null;e=S0(e))for(let i=Qe;i<e.length;i++){let r=e[i];bw(r,n)}}function Nk(t){for(let n=I0(t);n!==null;n=S0(n)){if(!(n[se]&2))continue;let e=n[Ur];for(let i=0;i<e.length;i++){let r=e[i];Eh(r)}}}function Fk(t,n,e){Oe(Ee.ComponentStart);let i=ln(n,t);try{bw(i,e)}finally{Oe(Ee.ComponentEnd,i[tt])}}function bw(t,n){ud(t)&&bp(t,n)}function bp(t,n){let i=t[ne],r=t[se],o=t[Xt],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&Do(o)),a||=!1,o&&(o.dirty=!1),t[se]&=-9217,a)Ok(i,t,i.template,t[tt]);else if(r&8192){let s=te(null);try{gw(t),vw(t,1);let l=i.components;l!==null&&yw(t,l,1),mw(t)}finally{te(s)}}}function yw(t,n,e){for(let i=0;i<n.length;i++)Fk(t,n[i],e)}function Pk(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Wi(~r);else{let o=r,a=e[++i],s=e[++i];dy(a,o);let l=n[o];Oe(Ee.HostBindingsUpdateStart,l);try{s(2,l)}finally{Oe(Ee.HostBindingsUpdateEnd,l)}}}}finally{Wi(-1)}}function fg(t,n){let e=Ph()?64:1088;for(t[En].changeDetectionScheduler?.notify(n);t;){t[se]|=e;let i=Hi(t);if(Bo(t)&&!i)return t;t=i}return null}function ww(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function Cw(t,n){let e=Qe+n;if(e<t.length)return t[e]}function Gs(t,n,e,i=!0){let r=n[ne];if(Lk(r,n,t,e),i){let a=vp(e,t),s=n[Be],l=s.parentNode(t[Gi]);l!==null&&tk(r,t[jt],s,n,l,a)}let o=n[jr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Dw(t,n){let e=Fs(t,n);return e!==void 0&&tu(e[ne],e),e}function Fs(t,n){if(t.length<=Qe)return;let e=Qe+n,i=t[e];if(i){let r=i[Ui];r!==null&&r!==t&&og(r,i),n>0&&(t[e-1][on]=i[on]);let o=ws(t,Qe+n);ek(i[ne],i);let a=o[qn];a!==null&&a.detachView(o[ne]),i[dt]=null,i[on]=null,i[se]&=-129}return i}function Lk(t,n,e,i){let r=Qe+i,o=e.length;i>0&&(e[r-1][on]=n),i<o-Qe?(n[on]=e[r],dh(e,Qe+i,n)):(e.push(n),n[on]=null),n[dt]=e;let a=n[Ui];a!==null&&e!==a&&xw(a,n);let s=n[qn];s!==null&&s.insertView(t),fd(n),n[se]|=128}function xw(t,n){let e=t[Ur],i=n[dt];if(gi(i))t[se]|=2;else{let r=i[dt][Vt];n[Vt]!==r&&(t[se]|=2)}e===null?t[Ur]=[n]:e.push(n)}var Qi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ne];return Ns(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[tt]}set context(n){this._lView[tt]=n}get destroyed(){return $r(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[dt];if(In(n)){let e=n[Ds],i=e?e.indexOf(this):-1;i>-1&&(Fs(n,i),ws(e,i))}this._attachedToViewContainer=!1}tu(this._lView[ne],this._lView)}onDestroy(n){Ih(this._lView,n)}markForCheck(){fg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[se]&=-129}reattach(){fd(this._lView),this._lView[se]|=128}detectChanges(){this._lView[se]|=1024,_w(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new S(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Bo(this._lView),e=this._lView[Ui];e!==null&&!n&&og(e,this._lView),nw(this._lView[ne],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new S(902,!1);this._appRef=n;let e=Bo(this._lView),i=this._lView[Ui];i!==null&&!e&&xw(i,this._lView),fd(this._lView)}};var ht=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=Bk;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=$s(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Qi(o)}}return t})();function Bk(){return ou(ft(),oe())}function ou(t,n){return t.type&4?new ht(n,t,Yo(t,n)):null}function Zo(t,n,e,i,r){let o=t.data[n];if(o===null)o=jk(t,n,e,i,r),cy()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=oy();o.injectorIndex=a===null?-1:a.injectorIndex}return jo(o,!0),o}function jk(t,n,e,i,r){let o=Oh(),a=Nh(),s=a?o:o&&o.parent,l=t.data[n]=Hk(t,s,e,n,i,r);return Vk(t,l,o,a),l}function Vk(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function Hk(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return kh()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Vh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function zk(t){let n=t[vh]??[],i=t[dt][Be],r=[];for(let o of n)o.data[R0]!==void 0?r.push(o):Uk(o,i);t[vh]=r}function Uk(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[k0];for(;e<r;){let o=i.nextSibling;G0(n,i,!1),i=o,e++}}}var $k=()=>null,Gk=()=>null;function Ld(t,n){return $k(t,n)}function Ew(t,n,e){return Gk(t,n,e)}var Iw=class{},au=class{},yp=class{resolveComponentFactory(n){throw new S(917,!1)}},Ws=class{static NULL=new yp},bt=class{},Te=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>Wk()}return t})();function Wk(){let t=oe(),n=ft(),e=ln(n.index,t);return(gi(e)?e:t)[Be]}var Sw=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>null})}return t})();var Td={},wp=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Td,i);return r!==Td||e===Td?r:this.parentInjector.get(n,e,i)}};function Bd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=ed(r,s);else if(o==2){let l=s,c=n[++a];i=ed(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function _e(t,n=0){let e=oe();if(e===null)return H(t,n);let i=ft();return y0(i,e,ct(t),n)}function su(){let t="invalid";throw new Error(t)}function Mw(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let u of a)if(u.resolveHostDirectives!==null){[s,l,c]=u.resolveHostDirectives(a);break}Yk(t,n,e,s,o,l,c)}o!==null&&i!==null&&qk(e,i,o)}function qk(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new S(-301,!1);i.push(n[r],o)}}function Qk(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function Yk(t,n,e,i,r,o,a){let s=i.length,l=null;for(let g=0;g<s;g++){let _=i[g];l===null&&Sn(_)&&(l=_,Qk(t,e,g)),ip(Fd(e,n),t,_.type)}tR(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let g=0;g<s;g++){let _=i[g];_.providersResolver&&_.providersResolver(_)}let c=!1,u=!1,f=X0(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let g=0;g<s;g++){let _=i[g];if(e.mergedAttrs=$o(e.mergedAttrs,_.hostAttrs),Zk(t,e,n,f,_),eR(f,_,r),a!==null&&a.has(_)){let[O,L]=a.get(_);e.directiveToIndex.set(_.type,[f,O+e.directiveStart,L+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,f);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let C=_.type.prototype;!c&&(C.ngOnChanges||C.ngOnInit||C.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(C.ngOnChanges||C.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}Kk(t,e,o)}function Kk(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))$y(0,n,r,i),$y(1,n,r,i),Wy(n,i,!1);else{let o=e.get(r);Gy(0,n,o,i),Gy(1,n,o,i),Wy(n,i,!0)}}}function $y(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),Tw(n,o)}}function Gy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),Tw(n,a)}}function Tw(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Wy(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Xp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){a??=[],a.push(c[u+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function Zk(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=ji(r.type,!0)),a=new Qr(o,Sn(r),_e,null);t.blueprint[i]=a,e[i]=a,Xk(t,n,i,X0(t,e,r.hostVars,qt),r)}function Xk(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;Jk(a)!=s&&a.push(s),a.push(e,i,o)}}function Jk(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function eR(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Sn(n)&&(e[""]=t)}}function tR(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function mg(t,n,e,i,r,o,a,s){let l=n[ne],c=l.consts,u=Jt(c,a),f=Zo(l,t,e,i,u);return o&&Mw(l,n,f,Jt(c,s),r),f.mergedAttrs=$o(f.mergedAttrs,f.attrs),f.attrs!==null&&Bd(f,f.attrs,!1),f.mergedAttrs!==null&&Bd(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function hg(t,n){u0(t,n),bh(n)&&t.queries.elementEnd(n)}function nR(t,n,e,i,r,o){let a=n.consts,s=Jt(a,r),l=Zo(n,t,e,i,s);if(l.mergedAttrs=$o(l.mergedAttrs,l.attrs),o!=null){let c=Jt(a,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Bd(l,l.attrs,!1),l.mergedAttrs!==null&&Bd(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function pg(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function iR(t,n,e){return t[n]=e}function en(t,n,e){if(e===qt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function rR(t,n,e,i){let r=en(t,n,e);return en(t,n+1,i)||r}function kd(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&eT(r,o);let a=Qn(t)?ln(t.index,n):n;fg(a,5);let s=n[tt],l=qy(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=qy(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function qy(t,n,e,i){let r=te(null);try{return Oe(Ee.OutputStart,n,e),e(i)!==!1}catch(o){return wk(t,o),!1}finally{Oe(Ee.OutputEnd,n,e),te(r)}}function kw(t,n,e,i,r,o,a,s){let l=Lo(t),c=!1,u=null;if(!i&&l&&(u=aR(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=a,u.__ngLastListenerFn__=a,c=!0}else{let f=sn(t,e),g=i?i(f):f;nT(e,g,o,s),i||(s.__ngNativeEl__=f);let _=r.listen(g,o,s);if(!oR(o)){let C=i?O=>i(an(O[t.index])):t.index;Rw(C,n,e,o,s,_,!1)}}return c}function oR(t){return t.startsWith("animation")||t.startsWith("transition")}function aR(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[Fo],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function Rw(t,n,e,i,r,o,a){let s=n.firstCreatePass?Mh(n):null,l=Sh(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function Qy(t,n,e,i,r,o){let a=n[e],s=n[ne],c=s.data[e].outputs[i],f=a[c].subscribe(o);Rw(t.index,s,n,r,o,f,!0)}var Cp=Symbol("BINDING");function Aw(t){return t.debugInfo?.className||t.type.name||null}var jd=class extends Ws{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=mi(n);return new Kr(e,this.ngModule)}};function sR(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Xd.SignalBased)!==0};return r&&(o.transform=r),o})}function lR(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function cR(t,n,e){let i=n instanceof Le?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new wp(e,i):e}function dR(t){let n=t.get(bt,null);if(n===null)throw new S(407,!1);let e=t.get(Sw,null),i=t.get(Wn,null),r=t.get(On,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function uR(t,n){let e=Ow(t);return U0(n,e,e==="svg"?yh:e==="math"?Zb:null)}function fR(t){if(t?.toLowerCase()==="script")throw new S(905,!1)}function Ow(t){return(t.selectors[0][0]||"div").toLowerCase()}var Kr=class extends au{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=sR(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=lR(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=zT(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Oe(Ee.DynamicComponentStart);let s=te(null);try{let l=this.componentDef,c=cR(l,r||this.ngModule,n),u=dR(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(Aw(l),()=>this.createComponentRef(u,c,e,i,o,a)):this.createComponentRef(u,c,e,i,o,a)}finally{te(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=mR(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),u=r?fk(c,r,s.encapsulation,e):uR(s,c);fR(u?.tagName);let f=a?.some(Yy)||o?.some(C=>typeof C!="function"&&C.bindings.some(Yy)),g=eg(null,l,null,512|Z0(s),null,null,n,c,e,null,N0(u,e,!0));g[We]=u,_d(g);let _=null;try{let C=mg(We,g,2,"#host",()=>l.directiveRegistry,!0,0);W0(c,u,C),Go(u,g),iu(l,g,C),Up(l,C,g),hg(l,C),i!==void 0&&pR(C,this.ngContentSelectors,i),_=ln(C.index,g),g[tt]=_[tt],ug(l,g,null)}catch(C){throw _!==null&&op(_),op(g),C}finally{Oe(Ee.DynamicComponentEnd),vd()}return new Vd(this.componentType,g,!!f)}};function mR(t,n,e,i){let r=t?["ng-version","21.2.18"]:UT(n.selectors[0]),o=null,a=null,s=0;if(e)for(let u of e)s+=u[Cp].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(a??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let g of f.bindings){s+=g[Cp].requiredVars;let _=u+1;g.create&&(g.targetIdx=_,(o??=[]).push(g)),g.update&&(g.targetIdx=_,(a??=[]).push(g))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,g=nd(f);l.push(g)}return Jp(0,null,hR(o,a),1,s,l,null,null,null,[r],null)}function hR(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Yy(t){let n=t[Cp].kind;return n==="input"||n==="twoWay"}var Vd=class extends Iw{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=dd(e[ne],We),this.location=Yo(this._tNode,e),this.instance=ln(this._tNode.index,e)[tt],this.hostView=this.changeDetectorRef=new Qi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=ru(i,r[ne],r,n,e);this.previousInputValues.set(n,e);let a=ln(i.index,r);fg(a,1)}get injector(){return new qr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function pR(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var pt=(()=>{class t{static __NG_ELEMENT_ID__=gR}return t})();function gR(){let t=ft();return Nw(t,oe())}var Dp=class t extends pt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Yo(this._hostTNode,this._hostLView)}get injector(){return new qr(this._hostTNode,this._hostLView)}get parentInjector(){let n=Vp(this._hostTNode,this._hostLView);if(h0(n)){let e=Od(n,this._hostLView),i=Ad(n),r=e[ne].data[i+8];return new qr(r,e)}else return new qr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Ky(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Qe}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=Ld(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,Wo(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!TM(n),c;if(l)c=e;else{let L=e||{};c=L.index,i=L.injector,r=L.projectableNodes,o=L.environmentInjector||L.ngModuleRef,a=L.directives,s=L.bindings}let u=l?n:new Kr(mi(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let Y=(l?f:this.parentInjector).get(Le,null);Y&&(o=Y)}let g=mi(u.componentType??{}),_=Ld(this._lContainer,g?.id??null),C=_?.firstChild??null,O=u.create(f,r,C,o,a,s);return this.insertImpl(O.hostView,c,Wo(this._hostTNode,_)),O}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Jb(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[dt],c=new t(l,l[jt],l[dt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Gs(a,r,o,i),n.attachToViewContainerRef(),dh(Yh(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Ky(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Fs(this._lContainer,e);i&&(ws(Yh(this._lContainer),e),tu(i[ne],i))}detach(n){let e=this._adjustIndex(n,-1),i=Fs(this._lContainer,e);return i&&ws(Yh(this._lContainer),e)!=null?new Qi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Ky(t){return t[Ds]}function Yh(t){return t[Ds]||(t[Ds]=[])}function Nw(t,n){let e,i=n[t.index];return In(i)?e=i:(e=ww(i,n,null,t),n[t.index]=e,tg(n,e)),vR(e,n,t,i),new Dp(e,t,n)}function _R(t,n){let e=t[Be],i=e.createComment(""),r=sn(n,t),o=e.parentNode(r);return Pd(e,o,i,e.nextSibling(r),!1),i}var vR=wR,bR=()=>!1;function yR(t,n,e){return bR(t,n,e)}function wR(t,n,e,i){if(t[Gi])return;let r;e.type&8?r=an(i):r=_R(n,e),t[Gi]=r}var xp=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Ep=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)_g(n,e).matches!==null&&this.queries[e].setDirty()}},Hd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=IR(n):this.predicate=n}},Ip=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Sp=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,CR(e,o)),this.matchTNodeWithReadOption(n,e,Md(e,n,o,!1,!1))}else i===ht?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Md(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===F||r===pt||r===ht&&e.type&4)this.addMatch(e.index,-2);else{let o=Md(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function CR(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function DR(t,n){return t.type&11?Yo(t,n):t.type&4?ou(t,n):null}function xR(t,n,e,i){return e===-1?DR(n,t):e===-2?ER(t,n,i):As(t,t[ne],e,n)}function ER(t,n,e){if(e===F)return Yo(n,t);if(e===ht)return ou(n,t);if(e===pt)return Nw(n,t)}function Fw(t,n,e,i){let r=n[qn].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let u=o[c];s.push(xR(n,u,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function Mp(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=Fw(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],u=n[-l];for(let f=Qe;f<u.length;f++){let g=u[f];g[Ui]===g[dt]&&Mp(g[ne],g,c,i)}if(u[Ur]!==null){let f=u[Ur];for(let g=0;g<f.length;g++){let _=f[g];Mp(_[ne],_,c,i)}}}}}return i}function gg(t,n){return t[qn].queries[n].queryList}function Pw(t,n,e){let i=new kn((e&4)===4);return ny(t,n,i,i.destroy),(n[qn]??=new Ep).queries.push(new xp(i))-1}function Lw(t,n,e){let i=Ve();return i.firstCreatePass&&(jw(i,new Hd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Pw(i,oe(),n)}function Bw(t,n,e,i){let r=Ve();if(r.firstCreatePass){let o=ft();jw(r,new Hd(n,e,i),o.index),SR(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Pw(r,oe(),e)}function IR(t){return t.split(",").map(n=>n.trim())}function jw(t,n,e){t.queries===null&&(t.queries=new Ip),t.queries.track(new Sp(n,e))}function SR(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function _g(t,n){return t.queries.getByIndex(n)}function Vw(t,n){let e=t[ne],i=_g(e,n);return i.crossesNgTemplate?Mp(e,t,n,[]):Fw(e,t,i,n)}function Hw(t,n,e){let i,r=ns(()=>{i._dirtyCounter();let o=MR(i,t);if(n&&o===void 0)throw new S(-951,!1);return o});return i=r[Ze],i._dirtyCounter=le(0),i._flatValue=void 0,r}function vg(t){return Hw(!0,!1,t)}function bg(t){return Hw(!0,!0,t)}function zw(t,n){let e=t[Ze];e._lView=oe(),e._queryIndex=n,e._queryList=gg(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function MR(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[se]&4)return n?void 0:Rt;let r=gg(e,i),o=Vw(e,i);return r.reset(o,D0),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Zn=class{},lu=class{};var zd=class extends Zn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new jd(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=sh(n);this._bootstrapComponents=Q0(o.bootstrap),this._r3Injector=Hh(n,e,[{provide:Zn,useValue:this},{provide:Ws,useValue:this.componentFactoryResolver},...i],_s(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Ud=class extends lu{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new zd(this.moduleType,n,[])}};var Ps=class extends Zn{injector;componentFactoryResolver=new jd(this);instance=null;constructor(n){super();let e=new Fr([...n.providers,{provide:Zn,useValue:this},{provide:Ws,useValue:this.componentFactoryResolver}],n.parent||No(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function qs(t,n,e=null){return new Ps({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var TR=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=mh(!1,e.type),r=i.length>0?qs([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t(H(Le))})}return t})();function I(t){return Bs(()=>{let n=Uw(t),e=X(b({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===Hp.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(TR).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Rn.Emulated,styles:t.styles||Rt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Ki("NgStandalone"),$w(e);let i=t.dependencies;return e.directiveDefs=Zy(i,kR),e.pipeDefs=Zy(i,Bb),e.id=OR(e),e})}function kR(t){return mi(t)||nd(t)}function T(t){return Bs(()=>({type:t.type,bootstrap:t.bootstrap||Rt,declarations:t.declarations||Rt,imports:t.imports||Rt,exports:t.exports||Rt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function RR(t,n){if(t==null)return Dn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Xd.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function AR(t){if(t==null)return Dn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function N(t){return Bs(()=>{let n=Uw(t);return $w(n),n})}function cu(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Uw(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Dn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Rt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:RR(t.inputs,n),outputs:AR(t.outputs),debugInfo:null}}function $w(t){t.features?.forEach(n=>n(t))}function Zy(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function OR(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function yg(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=NR,e.hostDirectives=i?t.map(Tp):[t]):i?e.hostDirectives.unshift(...t.map(Tp)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function NR(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,Gw(a,n,i),r.set(a,[s,n.length-1])}o===0&&Sn(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function Gw(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)Xy(Tp(o),n,e)}else Xy(i,n,e)}function Xy(t,n,e){let i=nd(t.directive);FR(i.declaredInputs,t.inputs),Gw(i,n,e),e.set(i,t),n.push(i)}function Tp(t){return typeof t=="function"?{directive:ct(t),inputs:Dn,outputs:Dn}:{directive:ct(t.directive),inputs:Jy(t.inputs),outputs:Jy(t.outputs)}}function Jy(t){if(t===void 0||t.length===0)return Dn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function FR(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function PR(t){return Object.getPrototypeOf(t.prototype).constructor}function ve(t){let n=PR(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,bs)?n[bs]:void 0,a=Object.hasOwn(n,ys)?n[ys]:void 0;if(Sn(t))r=o??a;else{if(o)throw new S(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=Kh(t.inputs),l.declaredInputs=Kh(t.declaredInputs),l.outputs=Kh(t.outputs);let c=r.hostBindings;c&&HR(t,c);let u=r.viewQuery,f=r.contentQueries;if(u&&jR(t,u),f&&VR(t,f),LR(t,r),Lb(t.outputs,r.outputs),Sn(r)&&r.data.animation){let g=t.data;g.animation=(g.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===ve&&(e=!1)}}n=Object.getPrototypeOf(n)}BR(i)}function LR(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function BR(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=$o(r.hostAttrs,e=$o(e,r.hostAttrs))}}function Kh(t){return t===Dn?{}:t===Rt?[]:t}function jR(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function VR(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function HR(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Ww(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=$o(t.mergedAttrs,t.attrs);let u=t.tView=Jp(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),jo(t,!1);let l=UR(e,n,t,i);bd()&&ag(e,n,l,t),Go(l,n);let c=ww(l,n,l,t);n[i+We]=c,tg(n,c),yR(c,t,n)}function zR(t,n,e,i,r,o,a,s,l,c,u){let f=e+We,g;return n.firstCreatePass?(g=Zo(n,f,4,a||null,s||null),md()&&Mw(n,t,g,Jt(n.consts,c),lg),u0(n,g)):g=n.data[f],Ww(g,t,n,e,i,r,o,l),Lo(g)&&iu(n,t,g),c!=null&&Us(t,g,u),g}function qo(t,n,e,i,r,o,a,s,l,c,u){let f=e+We,g;if(n.firstCreatePass){if(g=Zo(n,f,4,a||null,s||null),c!=null){let _=Jt(n.consts,c);g.localNames=[];for(let C=0;C<_.length;C+=2)g.localNames.push(_[C],-1)}}else g=n.data[f];return Ww(g,t,n,e,i,r,o,l),c!=null&&Us(t,g,u),g}function ze(t,n,e,i,r,o,a,s){let l=oe(),c=Ve(),u=Jt(c.consts,o);return zR(l,c,t,n,e,i,r,u,void 0,a,s),ze}function Qs(t,n,e,i,r,o,a,s){let l=oe(),c=Ve(),u=Jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,void 0,a,s),Qs}var UR=$R;function $R(t,n,e,i){return Ss(!0),n[Be].createComment("")}var du=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function wi(t){return typeof t=="function"&&t[Ze]!==void 0}function wg(t){return wi(t)&&typeof t.set=="function"}var Cg=new y("");function Zi(t){return!!t&&typeof t.then=="function"}function Dg(t){return!!t&&typeof t.subscribe=="function"}var uu=new y("");function fu(t){return xt([{provide:uu,multi:!0,useValue:t}])}var xg=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(uu,{optional:!0})??[];injector=d(B);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ut(this.injector,r);if(Zi(o))e.push(o);else if(Dg(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),mu=new y("");function qw(){Im(()=>{let t="";throw new S(600,t)})}function Qw(t){return t.isBoundToModule}var GR=10;var Et=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(cn);afterRenderManager=d(eu);zonelessEnabled=d(Ms);rootEffectScheduler=d(wd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new x;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(vi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(U(e=>!e))}constructor(){d(On,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Le);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=B.NULL){return this._injector.get(G).run(()=>{Oe(Ee.BootstrapComponentStart);let a=e instanceof au;if(!this._injector.get(xg).done){let C="";throw new S(405,C)}let l;a?l=e:l=this._injector.get(Ws).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=Qw(l)?void 0:this._injector.get(Zn),u=i||l.selector,f=l.create(r,[],u,c),g=f.location.nativeElement,_=f.injector.get(Cg,null);return _?.registerApplication(g),f.onDestroy(()=>{this.detachView(f.hostView),Rs(this.components,f),_?.unregisterApplication(g)}),this._loadComponent(f),Oe(Ee.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Oe(Ee.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Jd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Oe(Ee.ChangeDetectionEnd),new S(101,!1);let e=te(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,te(e),this.afterTick.next(),Oe(Ee.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(bt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<GR;){Oe(Ee.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Oe(Ee.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!xs(r))continue;let o=i&&!this.zonelessEnabled?0:1;_w(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>xs(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Rs(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(mu,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Rs(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new S(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Rs(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function Ys(t,n){let e=oe(),i=_i();if(en(e,i,n)){let r=Ve(),o=Vo();if(ru(o,r,e,t,n))Qn(o)&&dw(e,o.index);else{let s=sn(o,e);uw(e[Be],s,null,o.value,t,n,null)}}return Ys}function J(t,n,e,i){let r=oe(),o=_i();if(en(r,o,n)){let a=Ve(),s=Vo();bk(s,r,t,n,e,i)}return J}var kp=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Zh(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function WR(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){te(i);let c=n.length-1;for(te(null);a<=s&&a<=c;){let u=t.at(a),f=n[a],g=Zh(a,u,a,f,e);if(g!==0){g<0&&t.updateValue(a,f),a++;continue}let _=t.at(s),C=n[c],O=Zh(s,_,c,C,e);if(O!==0){O<0&&t.updateValue(s,C),s--,c--;continue}let L=e(a,u),Y=e(s,_),Me=e(a,f);if(Object.is(Me,Y)){let _t=e(c,C);Object.is(_t,L)?(t.swap(a,s),t.updateValue(s,C),c--,s--):t.move(s,a),t.updateValue(a,f),a++;continue}if(r??=new $d,o??=t0(t,a,s,e),Rp(t,r,a,Me))t.updateValue(a,f),a++,s++;else if(o.has(Me))r.set(L,t.detach(a)),s--;else{let _t=t.create(a,n[a]);t.attach(a,_t),a++,s++}}for(;a<=c;)e0(t,r,e,a,n[a]),a++}else if(n!=null){te(i);let c=n[Symbol.iterator]();te(null);let u=c.next();for(;!u.done&&a<=s;){let f=t.at(a),g=u.value,_=Zh(a,f,a,g,e);if(_!==0)_<0&&t.updateValue(a,g),a++,u=c.next();else{r??=new $d,o??=t0(t,a,s,e);let C=e(a,g);if(Rp(t,r,a,C))t.updateValue(a,g),a++,s++,u=c.next();else if(!o.has(C))t.attach(a,t.create(a,g)),a++,s++,u=c.next();else{let O=e(a,f);r.set(O,t.detach(a)),s--}}}for(;!u.done;)e0(t,r,e,t.length,u.value),u=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function Rp(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function e0(t,n,e,i,r){if(Rp(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function t0(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var $d=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function k(t,n,e,i,r,o,a,s){Ki("NgControlFlow");let l=oe(),c=Ve(),u=Jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,256,a,s),Eg}function Eg(t,n,e,i,r,o,a,s){Ki("NgControlFlow");let l=oe(),c=Ve(),u=Jt(c.consts,o);return qo(l,c,t,n,e,i,r,u,512,a,s),Eg}function R(t,n){Ki("NgControlFlow");let e=oe(),i=_i(),r=e[i]!==qt?e[i]:-1,o=r!==-1?Gd(e,We+r):void 0,a=0;if(en(e,i,t)){let s=te(null);try{if(o!==void 0&&Dw(o,a),t!==-1){let l=We+t,c=Gd(e,l),u=Fp(e[ne],l),f=Ew(c,u,e),g=$s(e,u,n,{dehydratedView:f});Gs(c,g,a,Wo(u,f))}}finally{te(s)}}else if(o!==void 0){let s=Cw(o,a);s!==void 0&&(s[tt]=n)}}var Ap=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Qe}};function Ks(t){return t}function un(t,n){return n}var Op=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function It(t,n,e,i,r,o,a,s,l,c,u,f,g){Ki("NgControlFlow");let _=oe(),C=Ve(),O=l!==void 0,L=oe(),Y=s?a.bind(L[Vt][tt]):a,Me=new Op(O,Y);L[We+t]=Me,qo(_,C,t+1,n,e,i,r,Jt(C.consts,o),256),O&&qo(_,C,t+2,l,c,u,f,Jt(C.consts,g),512)}var Np=class extends kp{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Qe}at(n){return this.getLView(n)[tt].$implicit}attach(n,e){let i=e[jr];this.needsIndexUpdate||=n!==this.length,Gs(this.lContainer,e,n,Wo(this.templateTNode,i)),qR(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,QR(this.lContainer,n),YR(this.lContainer,n)}create(n,e){let i=Ld(this.lContainer,this.templateTNode.tView.ssrId);return $s(this.hostLView,this.templateTNode,new Ap(this.lContainer,e,n),{dehydratedView:i})}destroy(n){tu(n[ne],n)}updateValue(n,e){this.getLView(n)[tt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[tt].$index=n}getLView(n){return KR(this.lContainer,n)}};function St(t){let n=te(null),e=Yn();try{let i=oe(),r=i[ne],o=i[e],a=e+1,s=Gd(i,a);if(o.liveCollection===void 0){let c=Fp(r,a);o.liveCollection=new Np(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(WR(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=_i(),u=l.length===0;if(en(i,c,u)){let f=e+2,g=Gd(i,f);if(u){let _=Fp(r,f),C=Ew(g,_,i),O=$s(i,_,void 0,{dehydratedView:C});Gs(g,O,0,Wo(_,C))}else r.firstUpdatePass&&zk(g),Dw(g,0)}}}finally{te(n)}}function Gd(t,n){return t[n]}function qR(t,n){if(t.length<=Qe)return;let e=Qe+n,i=t[e],r=i?i[$i]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[hi];XT(o,r),Yr.delete(i[pi]),r.detachedLeaveAnimationFns=void 0}}function QR(t,n){if(t.length<=Qe)return;let e=Qe+n,i=t[e],r=i?i[$i]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function YR(t,n){return Fs(t,n)}function KR(t,n){return Cw(t,n)}function Fp(t,n){return dd(t,n)}function j(t,n,e){let i=oe(),r=_i();if(en(i,r,n)){let o=Ve(),a=Vo();lw(a,i,t,n,i[Be],e)}return j}function Pp(t,n,e,i,r){ru(n,t,e,r?"class":"style",i)}function h(t,n,e,i){let r=oe(),o=r[ne],a=t+We,s=o.firstCreatePass?mg(a,r,2,n,lg,md(),e,i):o.data[a];if(Qn(s)){let l=r[En].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(Aw(c),()=>(n0(t,n,r,s,i),h))}}return n0(t,n,r,s,i),h}function n0(t,n,e,i,r){if(cg(i,e,t,n,Yw),Lo(i)){let o=e[ne];iu(o,e,i),Up(o,i,e)}r!=null&&Us(e,i)}function p(){let t=Ve(),n=ft(),e=dg(n);return t.firstCreatePass&&hg(t,e),Rh(e)&&Ah(),Th(),e.classesWithoutHost!=null&&FM(e)&&Pp(t,e,oe(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&PM(e)&&Pp(t,e,oe(),e.stylesWithoutHost,!1),p}function ie(t,n,e,i){return h(t,n,e,i),p(),ie}function nt(t,n,e,i){let r=oe(),o=r[ne],a=t+We,s=o.firstCreatePass?nR(a,o,2,n,e,i):o.data[a];return cg(s,r,t,n,Yw),i!=null&&Us(r,s),nt}function gt(){let t=ft(),n=dg(t);return Rh(n)&&Ah(),Th(),gt}function Qt(t,n,e,i){return nt(t,n,e,i),gt(),Qt}var Yw=(t,n,e,i,r)=>(Ss(!0),U0(n[Be],i,Vh()));function yt(t,n,e){let i=oe(),r=i[ne],o=t+We,a=r.firstCreatePass?mg(o,i,8,"ng-container",lg,md(),n,e):r.data[o];if(cg(a,i,t,"ng-container",ZR),Lo(a)){let s=i[ne];iu(s,i,a),Up(s,a,i)}return e!=null&&Us(i,a),yt}function wt(){let t=Ve(),n=ft(),e=dg(n);return t.firstCreatePass&&hg(t,e),wt}var ZR=(t,n,e,i,r)=>(Ss(!0),ST(n[Be],""));function it(){return oe()}function Mt(t,n,e){let i=oe(),r=_i();if(en(i,r,n)){let o=Ve(),a=Vo();cw(a,i,t,n,i[Be],e)}return Mt}var Zs="en-US";var XR=Zs;function Kw(t){typeof t=="string"&&(XR=t.toLowerCase().replace(/_/g,"-"))}function W(t,n,e){let i=oe(),r=Ve(),o=ft();return Zw(r,i,i[Be],o,t,n,e),W}function Xo(t,n,e){let i=oe(),r=Ve(),o=ft();return(o.type&3||e)&&kw(o,r,i,e,i[Be],t,n,kd(o,i,n)),Xo}function Zw(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=kd(i,n,o),kw(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let g=u[f],_=u[f+1];l??=kd(i,n,o),Qy(i,n,g,_,r,l)}if(c&&c.length)for(let f of c)l??=kd(i,n,o),Qy(i,n,f,r,r,l)}}function D(t=1){return gy(t)}function JR(t,n){let e=null,i=LT(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?K0(t,o,!0):VT(i,o))return r}return e}function me(t){let n=oe()[Vt][jt];if(!n.projection){let e=t?t.length:1,i=n.projection=$b(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?JR(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function z(t,n=0,e,i,r,o){let a=oe(),s=Ve(),l=i?t+1:null;l!==null&&qo(a,s,l,i,r,o,null,e);let c=Zo(s,We+t,16,null,e||null);c.projection===null&&(c.projection=n),Fh();let f=!a[jr]||kh();a[Vt][jt].projection[c.projection]===null&&l!==null?eA(a,s,l):f&&!Yd(c)&&ck(s,a,c)}function eA(t,n,e){let i=We+e,r=n.data[i],o=t[i],a=Ld(o,r.tView.ssrId),s=$s(t,r,void 0,{dehydratedView:a});Gs(o,s,0,Wo(r,a))}function Ye(t,n,e,i){return Bw(t,n,e,i),Ye}function Ne(t,n,e){return Lw(t,n,e),Ne}function q(t){let n=oe(),e=Ve(),i=gd();Es(i+1);let r=_g(e,i);if(t.dirty&&Xb(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Vw(n,i);t.reset(o,D0),t.notifyOnChanges()}return!0}return!1}function Q(){return gg(oe(),gd())}function hu(t,n,e,i,r){return zw(n,Bw(t,e,i,r)),hu}function pu(t,n,e,i){return zw(t,Lw(n,e,i)),pu}function gu(t=1){Es(gd()+t)}function Ct(t){let n=ay();return Ch(n,We+t)}function Ed(t,n){return t<<17|n<<2}function Zr(t){return t>>17&32767}function tA(t){return(t&2)==2}function nA(t,n){return t&131071|n<<17}function Lp(t){return t|2}function Qo(t){return(t&131068)>>2}function Xh(t,n){return t&-131069|n<<2}function iA(t){return(t&1)===1}function Bp(t){return t|1}function rA(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Zr(a),l=Qo(a);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||Oo(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let g=Zr(t[s+1]);t[i+1]=Ed(g,s),g!==0&&(t[g+1]=Xh(t[g+1],i)),t[s+1]=nA(t[s+1],i)}else t[i+1]=Ed(s,0),s!==0&&(t[s+1]=Xh(t[s+1],i)),s=i;else t[i+1]=Ed(l,0),s===0?s=i:t[l+1]=Xh(t[l+1],i),l=i;c&&(t[i+1]=Lp(t[i+1])),i0(t,u,i,!0),i0(t,u,i,!1),oA(n,u,t,i,o),a=Ed(s,l),o?n.classBindings=a:n.styleBindings=a}function oA(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Oo(o,n)>=0&&(e[i+1]=Bp(e[i+1]))}function i0(t,n,e,i){let r=t[e+1],o=n===null,a=i?Zr(r):Qo(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];aA(l,n)&&(s=!0,t[a+1]=i?Bp(c):Lp(c)),a=i?Zr(c):Qo(c)}s&&(t[e+1]=i?Lp(r):Bp(r))}function aA(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Oo(t,n)>=0:!1}var Tn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function sA(t){return t.substring(Tn.key,Tn.keyEnd)}function lA(t){return cA(t),Xw(t,Jw(t,0,Tn.textEnd))}function Xw(t,n){let e=Tn.textEnd;return e===n?-1:(n=Tn.keyEnd=dA(t,Tn.key=n,e),Jw(t,n,e))}function cA(t){Tn.key=0,Tn.keyEnd=0,Tn.value=0,Tn.valueEnd=0,Tn.textEnd=t.length}function Jw(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function dA(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Yt(t,n,e){return eC(t,n,e,!1),Yt}function P(t,n){return eC(t,n,null,!0),P}function at(t){fA(vA,uA,t,!0)}function uA(t,n){for(let e=lA(n);e>=0;e=Xw(n,e))ad(t,sA(n),!0)}function eC(t,n,e,i){let r=oe(),o=Ve(),a=hd(2);if(o.firstUpdatePass&&nC(o,t,a,i),n!==qt&&en(r,a,n)){let s=o.data[Yn()];iC(o,s,r,r[Be],t,r[a+1]=yA(n,e),i,a)}}function fA(t,n,e,i){let r=Ve(),o=hd(2);r.firstUpdatePass&&nC(r,null,o,i);let a=oe();if(e!==qt&&en(a,o,e)){let s=r.data[Yn()];if(rC(s,i)&&!tC(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=ed(l,e||"")),Pp(r,s,a,e,i)}else bA(r,s,a,a[Be],a[o+1],a[o+1]=_A(t,n,e),i,o)}}function tC(t,n){return n>=t.expandoStartIndex}function nC(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Yn()],a=tC(t,e);rC(o,i)&&n===null&&!a&&(n=!1),n=mA(r,o,n,i),rA(r,o,n,e,a,i)}}function mA(t,n,e,i){let r=fy(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Jh(null,t,n,e,i),e=Ls(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=Jh(r,t,n,e,i),o===null){let l=hA(t,n,i);l!==void 0&&Array.isArray(l)&&(l=Jh(null,t,n,l[1],i),l=Ls(l,n.attrs,i),pA(t,n,i,l))}else o=gA(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function hA(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Qo(i)!==0)return t[Zr(i)]}function pA(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Zr(r)]=i}function gA(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Ls(i,a,e)}return Ls(i,n.attrs,e)}function Jh(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Ls(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Ls(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),ad(t,a,e?!0:n[++o]))}return t===void 0?null:t}function _A(t,n,e){if(e==null||e==="")return Rt;let i=[],r=dn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function vA(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&ad(t,i,e)}function bA(t,n,e,i,r,o,a,s){r===qt&&(r=Rt);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let g=l<r.length?r[l+1]:void 0,_=c<o.length?o[c+1]:void 0,C=null,O;u===f?(l+=2,c+=2,g!==_&&(C=f,O=_)):f===null||u!==null&&u<f?(l+=2,C=u):(c+=2,C=f,O=_),C!==null&&iC(t,n,e,i,C,O,a,s),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function iC(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],u=iA(c)?r0(l,n,e,r,Qo(c),a):void 0;if(!Wd(u)){Wd(o)||tA(c)&&(o=r0(l,null,e,r,s,a));let f=wh(Yn(),e);uk(i,a,f,r,o)}}function r0(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,g=e[r+1];g===qt&&(g=f?Rt:void 0);let _=f?sd(g,i):u===i?g:void 0;if(c&&!Wd(_)&&(_=sd(l,i)),Wd(_)&&(s=_,a))return s;let C=t[r+1];r=a?Zr(C):Qo(C)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=sd(l,i))}return s}function Wd(t){return t!==void 0}function yA(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=_s(dn(t)))),t}function rC(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=oe(),i=Ve(),r=t+We,o=i.firstCreatePass?Zo(i,r,1,n,null):i.data[r],a=wA(i,e,o,n);e[r]=a,bd()&&ag(i,e,a,o),jo(o,!1)}var wA=(t,n,e,i)=>(Ss(!0),ET(n[Be],i));function CA(t,n,e,i=""){return en(t,_i(),e)?n+Lr(e)+i:qt}function DA(t,n,e,i,r,o=""){let a=sy(),s=rR(t,a,e,r);return hd(2),s?n+Lr(e)+i+Lr(r)+o:qt}function V(t){return E("",t),V}function E(t,n,e){let i=oe(),r=CA(i,t,n,e);return r!==qt&&oC(i,Yn(),r),E}function eo(t,n,e,i,r){let o=oe(),a=DA(o,t,n,e,i,r);return a!==qt&&oC(o,Yn(),a),eo}function oC(t,n,e){let i=wh(n,t);IT(t[Be],i,e)}function Xs(t,n,e){wg(n)&&(n=n());let i=oe(),r=_i();if(en(i,r,n)){let o=Ve(),a=Vo();lw(a,i,t,n,i[Be],e)}return Xs}function _u(t,n){let e=wg(t);return e&&t.set(n),e}function Js(t,n){let e=oe(),i=Ve(),r=ft();return Zw(i,e,e[Be],r,t,n),Js}function o0(t,n,e){let i=Ve();i.firstCreatePass&&aC(n,i.data,i.blueprint,Sn(t),e)}function aC(t,n,e,i,r){if(t=ct(t),Array.isArray(t))for(let o=0;o<t.length;o++)aC(t[o],n,e,i,r);else{let o=Ve(),a=oe(),s=ft(),l=Nr(t)?t:ct(t.provide),c=ph(t),u=s.providerIndexes&1048575,f=s.directiveStart,g=s.providerIndexes>>20;if(Nr(t)||!t.multi){let _=new Qr(c,r,_e,null),C=tp(l,n,r?u:u+g,f);C===-1?(ip(Fd(s,a),o,l),ep(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)):(e[C]=_,a[C]=_)}else{let _=tp(l,n,u+g,f),C=tp(l,n,u,u+g),O=_>=0&&e[_],L=C>=0&&e[C];if(r&&!L||!r&&!O){ip(Fd(s,a),o,l);let Y=IA(r?EA:xA,e.length,r,i,c,t);!r&&L&&(e[C].providerFactory=Y),ep(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(Y),a.push(Y)}else{let Y=sC(e[r?C:_],c,!r&&i);ep(o,t,_>-1?_:C,Y)}!r&&i&&L&&e[C].componentProviders++}}}function ep(t,n,e,i){let r=Nr(n),o=Qb(n);if(r||o){let l=(o?ct(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function sC(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function tp(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function xA(t,n,e,i,r){return jp(this.multi,[])}function EA(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=As(i,i[ne],this.providerFactory.index,r);a=l.slice(0,s),jp(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],jp(o,a);return a}function jp(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function IA(t,n,e,i,r,o){let a=new Qr(t,e,_e,null);return a.multi=[],a.index=n,a.componentProviders=0,sC(a,r,i&&!e),a}function Se(t,n){return e=>{e.providersResolver=(i,r)=>o0(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>o0(i,r?r(n):n,!0))}}function Ci(t,n,e){return lC(oe(),Lh(),t,n,e)}function SA(t,n){let e=t[n];return e===qt?void 0:e}function lC(t,n,e,i,r,o){let a=n+e;return en(t,a,r)?iR(t,a+1,o?i.call(o,r):i(r)):SA(t,a+1)}function Di(t,n){let e=Ve(),i,r=t+We;e.firstCreatePass?(i=MA(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=ji(i.type,!0)),a,s=Lt(_e);try{let l=Nd(!1),c=o();return Nd(l),Dh(e,oe(),r,c),c}finally{Lt(s)}}function MA(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function xi(t,n,e){let i=t+We,r=oe(),o=Ch(r,i);return TA(r,i)?lC(r,Lh(),n,o.transform,e,o):o.transform(e)}function TA(t,n){return t[ne].data[n].pure}function vu(t,n){return ou(t,n)}var qd=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},Ig=(()=>{class t{compileModuleSync(e){return new Ud(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=sh(e),o=Q0(r.declarations).reduce((a,s)=>{let l=mi(s);return l&&a.push(new Kr(l)),a},[]);return new qd(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var cC=(()=>{class t{applicationErrorHandler=d(cn);appRef=d(Et);taskService=d(vi);ngZone=d(G);zonelessEnabled=d(Ms);tracing=d(On,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new be;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ps):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Gh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?yy:zh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ps+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function dC(){return[{provide:Wn,useExisting:cC},{provide:G,useClass:gs},{provide:Ms,useValue:!0}]}function kA(){return typeof $localize<"u"&&$localize.locale||Zs}var bu=new y("",{factory:()=>d(bu,{optional:!0,skipSelf:!0})||kA()});function ke(t){return Ab(t)}function Ht(t,n){return ns(t,n?.equal)}var RA=t=>t;function Sg(t,n){if(typeof t=="function"){let e=Um(t,RA,n?.equal);return uC(e,n?.debugName)}else{let e=Um(t.source,t.computation,t.equal);return uC(e,t.debugName)}}function uC(t,n){let e=t[Ze],i=t;return i.set=r=>kb(e,r),i.update=r=>Rb(e,r),i.asReadonly=yd.bind(t),i}var vC=Symbol("InputSignalNode#UNSET"),qA=X(b({},is),{transformFn:void 0,applyValueToInputSignal(t,n){Cr(t,n)}});function bC(t,n){let e=Object.create(qA);e.value=t,e.transformFn=n?.transform;function i(){if(Ni(e),e.value===vC){let r=null;throw new S(-950,r)}return e.value}return i[Ze]=e,i}var fn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>js(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function fC(t,n){return bC(t,n)}function QA(t){return bC(vC,t)}var yC=(fC.required=QA,fC);function mC(t,n){return vg(n)}function YA(t,n){return bg(n)}var tl=(mC.required=YA,mC);function hC(t,n){return vg(n)}function KA(t,n){return bg(n)}var wC=(hC.required=KA,hC);var Tg=new y(""),ZA=new y("");function el(t){return!t.moduleRef}function XA(t){let n=el(t)?t.r3Injector:t.moduleRef.injector,e=n.get(G);return e.run(()=>{el(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(cn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),el(t)){let o=()=>n.destroy(),a=t.platformInjector.get(Tg);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(Tg);a.add(o),t.moduleRef.onDestroy(()=>{Rs(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return eO(i,e,()=>{let o=n.get(vi),a=o.add(),s=n.get(xg);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(bu,Zs);if(Kw(l||Zs),!n.get(ZA,!0))return el(t)?n.get(Et):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(el(t)){let u=n.get(Et);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return JA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var JA;function eO(t,n,e){try{let i=e();return Zi(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var yu=null;function tO(t=[],n){return B.create({name:n,providers:[{provide:Cs,useValue:"platform"},{provide:Tg,useValue:new Set([()=>yu=null])},...t]})}function nO(t=[]){if(yu)return yu;let n=tO(t);return yu=n,qw(),iO(n),n}function iO(t){let n=t.get(Qd,null);ut(t,()=>{n?.forEach(e=>e())})}function Ng(){return!1}var rO=1e4;var j9=rO-1e3;var ye=(()=>{class t{static __NG_ELEMENT_ID__=oO}return t})();function oO(t){return aO(ft(),oe(),(t&16)===16)}function aO(t,n,e){if(Qn(t)&&!e){let i=ln(t.index,n);return new Qi(i,i)}else if(t.type&175){let i=n[Vt];return new Qi(i,n)}return null}var kg=class{supports(n){return n instanceof Map||pg(n)}create(){return new Rg}},Rg=class{_records=new Map;_mapHead=null;_appendAfter=null;_previousMapHead=null;_changesHead=null;_changesTail=null;_additionsHead=null;_additionsTail=null;_removalsHead=null;get isDirty(){return this._additionsHead!==null||this._changesHead!==null||this._removalsHead!==null}forEachItem(n){let e;for(e=this._mapHead;e!==null;e=e._next)n(e)}forEachPreviousItem(n){let e;for(e=this._previousMapHead;e!==null;e=e._nextPrevious)n(e)}forEachChangedItem(n){let e;for(e=this._changesHead;e!==null;e=e._nextChanged)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}diff(n){if(!n)n=new Map;else if(!(n instanceof Map||pg(n)))throw new S(900,!1);return this.check(n)?this:null}check(n){this._reset();let e=this._mapHead;if(this._appendAfter=null,this._forEach(n,(i,r)=>{if(e&&e.key===r)this._maybeAddToChanges(e,i),this._appendAfter=e,e=e._next;else{let o=this._getOrCreateRecordForKey(r,i);e=this._insertBeforeOrAppend(e,o)}}),e){e._prev&&(e._prev._next=null),this._removalsHead=e;for(let i=e;i!==null;i=i._nextRemoved)i===this._mapHead&&(this._mapHead=null),this._records.delete(i.key),i._nextRemoved=i._next,i.previousValue=i.currentValue,i.currentValue=null,i._prev=null,i._next=null}return this._changesTail&&(this._changesTail._nextChanged=null),this._additionsTail&&(this._additionsTail._nextAdded=null),this.isDirty}_insertBeforeOrAppend(n,e){if(n){let i=n._prev;return e._next=n,e._prev=i,n._prev=e,i&&(i._next=e),n===this._mapHead&&(this._mapHead=e),this._appendAfter=n,n}return this._appendAfter?(this._appendAfter._next=e,e._prev=this._appendAfter):this._mapHead=e,this._appendAfter=e,null}_getOrCreateRecordForKey(n,e){if(this._records.has(n)){let r=this._records.get(n);this._maybeAddToChanges(r,e);let o=r._prev,a=r._next;return o&&(o._next=a),a&&(a._prev=o),r._next=null,r._prev=null,r}let i=new Ag(n);return this._records.set(n,i),i.currentValue=e,this._addToAdditions(i),i}_reset(){if(this.isDirty){let n;for(this._previousMapHead=this._mapHead,n=this._previousMapHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._changesHead;n!==null;n=n._nextChanged)n.previousValue=n.currentValue;for(n=this._additionsHead;n!=null;n=n._nextAdded)n.previousValue=n.currentValue;this._changesHead=this._changesTail=null,this._additionsHead=this._additionsTail=null,this._removalsHead=null}}_maybeAddToChanges(n,e){Object.is(e,n.currentValue)||(n.previousValue=n.currentValue,n.currentValue=e,this._addToChanges(n))}_addToAdditions(n){this._additionsHead===null?this._additionsHead=this._additionsTail=n:(this._additionsTail._nextAdded=n,this._additionsTail=n)}_addToChanges(n){this._changesHead===null?this._changesHead=this._changesTail=n:(this._changesTail._nextChanged=n,this._changesTail=n)}_forEach(n,e){n instanceof Map?n.forEach(e):Object.keys(n).forEach(i=>e(n[i],i))}},Ag=class{key;previousValue=null;currentValue=null;_nextPrevious=null;_next=null;_prev=null;_nextAdded=null;_nextRemoved=null;_nextChanged=null;constructor(n){this.key=n}};function pC(){return new Fg([new kg])}var Fg=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:pC});factories;constructor(e){this.factories=e}static create(e,i){if(i){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||pC())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i)return i;throw new S(901,!1)}}return t})();function CC(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Oe(Ee.BootstrapApplicationStart);try{let o=r?.injector??nO(i),a=[dC(),Cy,...e||[]],s=new Ps({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return XA({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Oe(Ee.BootstrapApplicationEnd)}}function ee(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function zt(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Mg=Symbol("NOT_SET"),DC=new Set,sO=X(b({},is),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Mg,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Mg&&!Do(this))return this.signal;try{for(let r of this.cleanup??DC)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=si(this),i;try{i=this.userFn.apply(null,n)}finally{Fi(this,e)}return(this.value===Mg||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Og=class extends Os{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(mt),a),this.scheduler=r;for(let s of ig){let l=e[s];if(l===void 0)continue;let c=Object.create(sO);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(Ni(c),c.value),c.signal[Ze]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[s]=c,this.hooks[s]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??DC)e()}finally{Pi(n)}}};function xC(t,n){let e=n?.injector??d(B),i=e.get(Wn),r=e.get(eu),o=e.get(On,null,{optional:!0});r.impl??=e.get(rg);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(Ho,null,{optional:!0}),l=new Og(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Cu(t,n){let e=mi(t),i=n.elementInjector||No();return new Kr(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var EC=null;function mn(){return EC}function Pg(t){EC??=t}var nl=class{},Jo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(IC),providedIn:"platform"})}return t})();var IC=(()=>{class t extends Jo{_location;_history;_doc=d(K);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return mn().getBaseHref(this._doc)}onPopState(e){let i=mn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=mn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function TC(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function SC(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Ji(t){return t&&t[0]!=="?"?`?${t}`:t}var ea=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(cO),providedIn:"root"})}return t})(),lO=new y(""),cO=(()=>{class t extends ea{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(K).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return TC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Ji(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Ji(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Ji(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(H(Jo),H(lO,8))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var er=(()=>{class t{_subject=new x;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=fO(SC(MC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Ji(i))}normalize(e){return t.stripTrailingSlash(uO(this._basePath,MC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Ji(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Ji(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Ji;static joinWithSlash=TC;static stripTrailingSlash=SC;static \u0275fac=function(i){return new(i||t)(H(ea))};static \u0275prov=w({token:t,factory:()=>dO(),providedIn:"root"})}return t})();function dO(){return new er(H(ea))}function uO(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function MC(t){return t.replace(/\/index\.html$/,"")}function fO(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Lg=/\s+/,kC=[],Bg=(()=>{class t{_ngEl;_renderer;initialClasses=kC;rawClass;stateMap=new Map;constructor(e,i){this._ngEl=e,this._renderer=i}set klass(e){this.initialClasses=e!=null?e.trim().split(Lg):kC}set ngClass(e){this.rawClass=typeof e=="string"?e.trim().split(Lg):e}ngDoCheck(){for(let i of this.initialClasses)this._updateState(i,!0);let e=this.rawClass;if(Array.isArray(e)||e instanceof Set)for(let i of e)this._updateState(i,!0);else if(e!=null)for(let i of Object.keys(e))this._updateState(i,!!e[i]);this._applyStateDiff()}_updateState(e,i){let r=this.stateMap.get(e);r!==void 0?(r.enabled!==i&&(r.changed=!0,r.enabled=i),r.touched=!0):this.stateMap.set(e,{enabled:i,changed:!0,touched:!0})}_applyStateDiff(){for(let e of this.stateMap){let i=e[0],r=e[1];r.changed?(this._toggleClass(i,r.enabled),r.changed=!1):r.touched||(r.enabled&&this._toggleClass(i,!1),this.stateMap.delete(i)),r.touched=!1}}_toggleClass(e,i){e=e.trim(),e.length>0&&e.split(Lg).forEach(r=>{i?this._renderer.addClass(this._ngEl.nativeElement,r):this._renderer.removeClass(this._ngEl.nativeElement,r)})}static \u0275fac=function(i){return new(i||t)(_e(F),_e(Te))};static \u0275dir=N({type:t,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return t})();var jg=(()=>{class t{_ngEl;_differs;_renderer;_ngStyle=null;_differ=null;constructor(e,i,r){this._ngEl=e,this._differs=i,this._renderer=r}set ngStyle(e){this._ngStyle=e,!this._differ&&e&&(this._differ=this._differs.find(e).create())}ngDoCheck(){if(this._differ){let e=this._differ.diff(this._ngStyle);e&&this._applyChanges(e)}}_setStyle(e,i){let[r,o]=e.split("."),a=r.indexOf("-")===-1?void 0:An.DashCase;i!=null?this._renderer.setStyle(this._ngEl.nativeElement,r,o?`${i}${o}`:i,a):this._renderer.removeStyle(this._ngEl.nativeElement,r,a)}_applyChanges(e){e.forEachRemovedItem(i=>this._setStyle(i.key,null)),e.forEachAddedItem(i=>this._setStyle(i.key,i.currentValue)),e.forEachChangedItem(i=>this._setStyle(i.key,i.currentValue))}static \u0275fac=function(i){return new(i||t)(_e(F),_e(Fg),_e(Te))};static \u0275dir=N({type:t,selectors:[["","ngStyle",""]],inputs:{ngStyle:"ngStyle"}})}return t})(),Vg=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(B);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(_e(pt))};static \u0275dir=N({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ue]})}return t})();var Du=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();function il(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var to=class{};var Hg="browser";function RC(t){return t===Hg}var rl=class{_doc;constructor(n){this._doc=n}manager},xu=(()=>{class t extends rl{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(H(K))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Su=new y(""),Gg=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof xu));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof xu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new S(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(H(Su),H(G))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),zg="ng-app-id";function OC(t){for(let n of t)n.remove()}function NC(t,n){let e=n.createElement("style");return e.textContent=t,e}function pO(t,n,e,i){let r=t.head?.querySelectorAll(`style[${zg}="${n}"],link[${zg}="${n}"]`);if(r)for(let o of r)o.removeAttribute(zg),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function $g(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Wg=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,pO(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,NC);i?.forEach(r=>this.addUsage(r,this.external,$g))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(OC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])OC(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,NC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,$g(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(H(K),H(Yi),H(Jr,8),H(Xr))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Ug={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},qg=/%COMP%/g;var PC="%COMP%",gO=`_nghost-${PC}`,_O=`_ngcontent-${PC}`,vO=!0,bO=new y("",{factory:()=>vO});function yO(t){return _O.replace(qg,t)}function wO(t){return gO.replace(qg,t)}function LC(t,n){return n.map(e=>e.replace(qg,t))}var Qg=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new ol(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Iu?r.applyToHost(e):r instanceof al&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Rn.Emulated:o=new Iu(l,c,i,this.appId,u,a,s,f);break;case Rn.ShadowDom:return new Eu(l,e,i,a,s,this.nonce,f,c);case Rn.ExperimentalIsolatedShadowDom:return new Eu(l,e,i,a,s,this.nonce,f);default:o=new al(l,c,i,u,a,s,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(H(Gg),H(Wg),H(Yi),H(bO),H(K),H(G),H(Jr),H(On,8))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),ol=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Ug[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(FC(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(FC(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new S(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Ug[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Ug[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(An.DashCase|An.Important)?n.style.setProperty(e,i,r&An.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&An.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=mn().getGlobalEventTarget(this.doc,n),!n))throw new S(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function FC(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Eu=class extends ol{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=LC(i.id,c);for(let f of c){let g=document.createElement("style");a&&g.setAttribute("nonce",a),g.textContent=f,this.shadowRoot.appendChild(g)}let u=i.getExternalStyles?.();if(u)for(let f of u){let g=$g(f,r);a&&g.setAttribute("nonce",a),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},al=class extends ol{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?LC(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Yr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Iu=class extends al{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=yO(c),this.hostAttr=wO(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Mu=class t extends nl{supportsDOMEvents=!0;static makeCurrent(){Pg(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=CO();return e==null?null:DO(e)}resetBaseElement(){sl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return il(document.cookie,n)}},sl=null;function CO(){return sl=sl||document.head.querySelector("base"),sl?sl.getAttribute("href"):null}function DO(t){return new URL(t,document.baseURI).pathname}var xO=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),BC=["alt","control","meta","shift"],EO={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},IO={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},jC=(()=>{class t extends rl{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>mn().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),BC.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=EO[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),BC.forEach(a=>{if(a!==r){let s=IO[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(H(K))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function Yg(t,n,e){let i=b({rootComponent:t},SO(n,e));return CC(i)}function SO(t,n){return{platformRef:n?.platformRef,appProviders:[...AO,...t?.providers??[]],platformProviders:RO}}function MO(){Mu.makeCurrent()}function TO(){return new Bt}function kO(){return zp(document),document}var RO=[{provide:Xr,useValue:Hg},{provide:Qd,useValue:MO,multi:!0},{provide:K,useFactory:kO}];var AO=[{provide:Cs,useValue:"root"},{provide:Bt,useFactory:TO},{provide:Su,useClass:xu,multi:!0},{provide:Su,useClass:jC,multi:!0},Qg,Wg,Gg,{provide:bt,useExisting:Qg},{provide:to,useClass:xO},[]];var tr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=this.headers.get(e);if(!a)return;a=a.filter(s=>o.indexOf(s)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var ku=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Ru=class{encodeKey(n){return VC(n)}encodeValue(n){return VC(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function OO(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var NO=/%(\d[a-f0-9])/gi,FO={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function VC(t){return encodeURIComponent(t).replace(NO,(n,e)=>FO[e]??n)}function Tu(t){return`${t}`}var Ei=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Ru,n.fromString){if(n.fromObject)throw new S(2805,!1);this.map=OO(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Tu):[Tu(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Tu(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Tu(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function PO(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function HC(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function zC(t){return typeof Blob<"u"&&t instanceof Blob}function UC(t){return typeof FormData<"u"&&t instanceof FormData}function LO(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var $C="Content-Type",GC="Accept",qC="text/plain",QC="application/json",BO=`${QC}, ${qC}, */*`,ta=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(PO(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new S(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new tr,this.context??=new ku,!this.params)this.params=new Ei,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||HC(this.body)||zC(this.body)||UC(this.body)||LO(this.body)?this.body:this.body instanceof Ei?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||UC(this.body)?null:zC(this.body)?this.body.type||null:HC(this.body)?null:typeof this.body=="string"?qC:this.body instanceof Ei?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?QC:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer??this.referrer,g=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,C=n.transferCache??this.transferCache,O=n.timeout??this.timeout,L=n.body!==void 0?n.body:this.body,Y=n.withCredentials??this.withCredentials,Me=n.reportProgress??this.reportProgress,_t=n.headers||this.headers,vt=n.params||this.params,Xa=n.context??this.context;return n.setHeaders!==void 0&&(_t=Object.keys(n.setHeaders).reduce((Ja,vr)=>Ja.set(vr,n.setHeaders[vr]),_t)),n.setParams&&(vt=Object.keys(n.setParams).reduce((Ja,vr)=>Ja.set(vr,n.setParams[vr]),vt)),new t(e,i,L,{params:vt,headers:_t,context:Xa,reportProgress:Me,responseType:r,withCredentials:Y,transferCache:C,keepalive:o,cache:s,priority:a,timeout:O,mode:l,redirect:c,credentials:u,referrer:f,integrity:g,referrerPolicy:_})}},no=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(no||{}),ia=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new tr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Au=class t extends ia{constructor(n={}){super(n)}type=no.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},ll=class t extends ia{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=no.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},na=class extends ia{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},jO=200,VO=204;var HO=new y("");var zO=/^\)\]\}',?\n/;var Zg=(()=>{class t{xhrFactory;tracingService=d(On,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new S(-2800,!1);let i=this.xhrFactory;return Z(null).pipe(Ie(()=>new re(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((L,Y)=>a.setRequestHeader(L,Y.join(","))),e.headers.has(GC)||a.setRequestHeader(GC,BO),!e.headers.has($C)){let L=e.detectContentTypeHeader();L!==null&&a.setRequestHeader($C,L)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let L=e.responseType.toLowerCase();a.responseType=L!=="json"?L:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let L=a.statusText||"OK",Y=new tr(a.getAllResponseHeaders()),Me=a.responseURL||e.url;return l=new Au({headers:Y,status:a.status,statusText:L,url:Me}),l},u=this.maybePropagateTrace(()=>{let{headers:L,status:Y,statusText:Me,url:_t}=c(),vt=null;Y!==VO&&(vt=typeof a.response>"u"?a.responseText:a.response),Y===0&&(Y=vt?jO:0);let Xa=Y>=200&&Y<300;if(e.responseType==="json"&&typeof vt=="string"){let Ja=vt;vt=vt.replace(zO,"");try{vt=vt!==""?JSON.parse(vt):null}catch(vr){vt=Ja,Xa&&(Xa=!1,vt={error:vr,text:vt})}}Xa?(o.next(new ll({body:vt,headers:L,status:Y,statusText:Me,url:_t||void 0})),o.complete()):o.error(new na({error:vt,headers:L,status:Y,statusText:Me,url:_t||void 0}))}),f=this.maybePropagateTrace(L=>{let{url:Y}=c(),Me=new na({error:L,status:a.status||0,statusText:a.statusText||"Unknown Error",url:Y||void 0});o.error(Me)}),g=f;e.timeout&&(g=this.maybePropagateTrace(L=>{let{url:Y}=c(),Me=new na({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:Y||void 0});o.error(Me)}));let _=!1,C=this.maybePropagateTrace(L=>{_||(o.next(c()),_=!0);let Y={type:no.DownloadProgress,loaded:L.loaded};L.lengthComputable&&(Y.total=L.total),e.responseType==="text"&&a.responseText&&(Y.partialText=a.responseText),o.next(Y)}),O=this.maybePropagateTrace(L=>{let Y={type:no.UploadProgress,loaded:L.loaded};L.lengthComputable&&(Y.total=L.total),o.next(Y)});return a.addEventListener("load",u),a.addEventListener("error",f),a.addEventListener("timeout",g),a.addEventListener("abort",f),e.reportProgress&&(a.addEventListener("progress",C),s!==null&&a.upload&&a.upload.addEventListener("progress",O)),a.send(s),o.next({type:no.Sent}),()=>{a.removeEventListener("error",f),a.removeEventListener("abort",f),a.removeEventListener("load",u),a.removeEventListener("timeout",g),e.reportProgress&&(a.removeEventListener("progress",C),s!==null&&a.upload&&a.upload.removeEventListener("progress",O)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(H(to))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function YC(t,n){return n(t)}function UO(t,n){return(e,i)=>n.intercept(e,{handle:r=>t(r,i)})}function $O(t,n,e){return(i,r)=>ut(e,()=>n(i,o=>t(o,r)))}var KC=new y(""),Xg=new y("",{factory:()=>[]}),ZC=new y(""),Jg=new y("",{factory:()=>!0});function GO(){let t=null;return(n,e)=>{t===null&&(t=(d(KC,{optional:!0})??[]).reduceRight(UO,YC));let i=d(Wr);if(d(Jg)){let o=i.add();return t(n,e).pipe(Bi(o))}else return t(n,e)}}var e_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=H(Zg),r},providedIn:"root"})}return t})();var Ou=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Wr);contributeToStability=d(Jg);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Xg),...this.injector.get(ZC,[])]));this.chain=i.reduceRight((r,o)=>$O(r,o,this.injector),YC)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Bi(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(H(e_),H(Le))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),t_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=H(Ou),r},providedIn:"root"})}return t})();function Kg(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var ra=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof ta)o=e;else{let l;r.headers instanceof tr?l=r.headers:l=new tr(r.headers);let c;r.params&&(r.params instanceof Ei?c=r.params:c=new Ei({fromObject:r.params})),o=new ta(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=Z(o).pipe(ko(l=>this.handler.handle(l)));if(e instanceof ta||r.observe==="events")return a;let s=a.pipe(ue(l=>l instanceof ll));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(U(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new S(2806,!1);return l.body}));case"blob":return s.pipe(U(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new S(2807,!1);return l.body}));case"text":return s.pipe(U(l=>{if(l.body!==null&&typeof l.body!="string")throw new S(2808,!1);return l.body}));default:return s.pipe(U(l=>l.body))}case"response":return s;default:throw new S(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Ei().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Kg(r,i))}post(e,i,r={}){return this.request("POST",e,Kg(r,i))}put(e,i,r={}){return this.request("PUT",e,Kg(r,i))}static \u0275fac=function(i){return new(i||t)(H(t_))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var WO=new y("",{factory:()=>!0}),qO="XSRF-TOKEN",QO=new y("",{factory:()=>qO}),YO="X-XSRF-TOKEN",KO=new y("",{factory:()=>YO}),ZO=(()=>{class t{cookieName=d(QO);doc=d(K);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=il(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),XC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=H(ZO),r},providedIn:"root"})}return t})();function XO(t,n){if(!d(WO)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Jo).href,{origin:o}=new URL(r),{origin:a}=new URL(t.url,o);if(o!==a)return n(t)}catch{return n(t)}let e=d(XC).getToken(),i=d(KO);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var n_=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t})(n_||{});function JO(t,n){return{\u0275kind:t,\u0275providers:n}}function i_(...t){let n=[ra,Ou,{provide:t_,useExisting:Ou},{provide:e_,useFactory:()=>d(HO,{optional:!0})??d(Zg)},{provide:Xg,useValue:XO,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return xt(n)}var WC=new y("");function r_(){return JO(n_.LegacyInterceptors,[{provide:WC,useFactory:GO},{provide:Xg,useExisting:WC,multi:!0}])}var JC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(H(K))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var cl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=H(t1),r},providedIn:"root"})}return t})(),t1=(()=>{class t extends cl{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case ot.NONE:return i;case ot.HTML:return Xn(i,"HTML")?dn(i):Zd(this._doc,String(i)).toString();case ot.STYLE:return Xn(i,"Style")?dn(i):i;case ot.SCRIPT:if(Xn(i,"Script"))return dn(i);throw new S(5200,!1);case ot.URL:return Xn(i,"URL")?dn(i):Hs(String(i));case ot.RESOURCE_URL:if(Xn(i,"ResourceURL"))return dn(i);throw new S(5201,!1);default:throw new S(5202,!1)}}bypassSecurityTrustHtml(e){return $p(e)}bypassSecurityTrustStyle(e){return Gp(e)}bypassSecurityTrustScript(e){return Wp(e)}bypassSecurityTrustUrl(e){return qp(e)}bypassSecurityTrustResourceUrl(e){return Qp(e)}static \u0275fac=function(i){return new(i||t)(H(K))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var he="primary",Dl=Symbol("RouteTitle"),c_=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function ro(t){return new c_(t)}function o_(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function lD(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return o_(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!o_(o,t.slice(0,o.length),s)||!o_(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function ju(t){return new Promise((n,e)=>{t.pipe(ui()).subscribe({next:i=>n(i),error:i=>e(i)})})}function n1(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Jn(t[e],n[e]))return!1;return!0}function Jn(t,n){let e=t?d_(t):void 0,i=n?d_(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!cD(t[r],n[r]))return!1;return!0}function d_(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function cD(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function i1(t){return t.length>0?t[t.length-1]:null}function so(t){return ls(t)?t:Zi(t)?je(Promise.resolve(t)):Z(t)}function dD(t){return ls(t)?ju(t):Promise.resolve(t)}var r1={exact:mD,subset:hD},uD={exact:o1,subset:a1,ignored:()=>!0},fD={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},u_={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function tD(t,n,e){return r1[e.paths](t.root,n.root,e.matrixParams)&&uD[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function o1(t,n){return Jn(t,n)}function mD(t,n,e){if(!io(t.segments,n.segments)||!Pu(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!mD(t.children[i],n.children[i],e))return!1;return!0}function a1(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>cD(t[e],n[e]))}function hD(t,n,e){return pD(t,n,n.segments,e)}function pD(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!io(r,e)||n.hasChildren()||!Pu(r,e,i))}else if(t.segments.length===e.length){if(!io(t.segments,e)||!Pu(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!hD(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!io(t.segments,r)||!Pu(t.segments,r,i)||!t.children[he]?!1:pD(t.children[he],n,o,i)}}function Pu(t,n,e){return n.every((i,r)=>uD[e](t[r].parameters,i.parameters))}var nn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Re([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=ro(this.queryParams),this._queryParamMap}toString(){return c1.serialize(this)}},Re=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Lu(this)}},nr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=ro(this.parameters),this._parameterMap}toString(){return _D(this)}};function s1(t,n){return io(t,n)&&t.every((e,i)=>Jn(e.parameters,n[i].parameters))}function io(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function l1(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===he&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==he&&(e=e.concat(n(r,i)))}),e}var ma=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new ir,providedIn:"root"})}return t})(),ir=class{parse(n){let e=new m_(n);return new nn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${dl(n.root,!0)}`,i=f1(n.queryParams),r=typeof n.fragment=="string"?`#${d1(n.fragment)}`:"";return`${e}${i}${r}`}},c1=new ir;function Lu(t){return t.segments.map(n=>_D(n)).join("/")}function dl(t,n){if(!t.hasChildren())return Lu(t);if(n){let e=t.children[he]?dl(t.children[he],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==he&&i.push(`${r}:${dl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=l1(t,(i,r)=>r===he?[dl(t.children[he],!1)]:[`${r}:${dl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[he]!=null?`${Lu(t)}/${e[0]}`:`${Lu(t)}/(${e.join("//")})`}}function gD(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Nu(t){return gD(t).replace(/%3B/gi,";")}function d1(t){return encodeURI(t)}function f_(t){return gD(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Bu(t){return decodeURIComponent(t)}function nD(t){return Bu(t.replace(/\+/g,"%20"))}function _D(t){return`${f_(t.path)}${u1(t.parameters)}`}function u1(t){return Object.entries(t).map(([n,e])=>`;${f_(n)}=${f_(e)}`).join("")}function f1(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Nu(e)}=${Nu(r)}`).join("&"):`${Nu(e)}=${Nu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var m1=/^[^\/()?;#]+/;function a_(t){let n=t.match(m1);return n?n[0]:""}var h1=/^[^\/()?;=#]+/;function p1(t){let n=t.match(h1);return n?n[0]:""}var g1=/^[^=?&#]+/;function _1(t){let n=t.match(g1);return n?n[0]:""}var v1=/^[^&#]+/;function b1(t){let n=t.match(v1);return n?n[0]:""}var m_=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Re([],{}):new Re([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new S(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[he]=new Re(e,i)),r}parseSegment(){let n=a_(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new S(4009,!1);return this.capture(n),new nr(Bu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=p1(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=a_(this.remaining);r&&(i=r,this.capture(i))}n[Bu(e)]=Bu(i)}parseQueryParam(n){let e=_1(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=b1(this.remaining);a&&(i=a,this.capture(i))}let r=nD(e),o=nD(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=a_(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new S(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=he);let s=this.parseChildren(e+1);i[a??he]=Object.keys(s).length===1&&s[he]?s[he]:new Re([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new S(4011,!1)}};function vD(t){return t.segments.length>0?new Re([],{[he]:t}):t}function bD(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=bD(r);if(i===he&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Re(t.segments,n);return y1(e)}function y1(t){if(t.numberOfChildren===1&&t.children[he]){let n=t.children[he];return new Re(t.segments.concat(n.segments),n.children)}return t}function rr(t){return t instanceof nn}function yD(t,n,e=null,i=null,r=new ir){let o=wD(t);return CD(o,n,e,i,r)}function wD(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new Re(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=vD(i);return n??r}function CD(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return s_(o,o,o,e,i,r);let a=w1(n);if(a.toRoot())return s_(o,o,new Re([],{}),e,i,r);let s=C1(a,o,t),l=s.processChildren?fl(s.segmentGroup,s.index,a.commands):xD(s.segmentGroup,s.index,a.commands);return s_(o,s.segmentGroup,l,e,i,r)}function Vu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function pl(t){return typeof t=="object"&&t!=null&&t.outlets}function iD(t,n,e){t||="\u0275";let i=new nn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function s_(t,n,e,i,r,o){let a={};for(let[c,u]of Object.entries(i??{}))a[c]=Array.isArray(u)?u.map(f=>iD(c,f,o)):iD(c,u,o);let s;t===n?s=e:s=DD(t,n,e);let l=vD(bD(s));return new nn(l,a,r)}function DD(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=DD(o,n,e)}),new Re(t.segments,i)}var Hu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Vu(i[0]))throw new S(4003,!1);let r=i.find(pl);if(r&&r!==i1(i))throw new S(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function w1(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Hu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new Hu(e,n,i)}var aa=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function C1(t,n,e){if(t.isAbsolute)return new aa(n,!0,0);if(!e)return new aa(n,!1,NaN);if(e.parent===null)return new aa(e,!0,0);let i=Vu(t.commands[0])?0:1,r=e.segments.length-1+i;return D1(e,r,t.numberOfDoubleDots)}function D1(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new S(4005,!1);r=i.segments.length}return new aa(i,!1,r-o)}function x1(t){return pl(t[0])?t[0].outlets:{[he]:t}}function xD(t,n,e){if(t??=new Re([],{}),t.segments.length===0&&t.hasChildren())return fl(t,n,e);let i=E1(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Re(t.segments.slice(0,i.pathIndex),{});return o.children[he]=new Re(t.segments.slice(i.pathIndex),t.children),fl(o,0,r)}else return i.match&&r.length===0?new Re(t.segments,{}):i.match&&!t.hasChildren()?h_(t,n,e):i.match?fl(t,0,r):h_(t,n,e)}function fl(t,n,e){if(e.length===0)return new Re(t.segments,{});{let i=x1(e),r={};if(Object.keys(i).some(o=>o!==he)&&t.children[he]&&t.numberOfChildren===1&&t.children[he].segments.length===0){let o=fl(t.children[he],n,e);return new Re(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=xD(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new Re(t.segments,r)}}function E1(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(pl(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!oD(l,c,a))return o;i+=2}else{if(!oD(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function h_(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(pl(o)){let l=I1(o.outlets);return new Re(i,l)}if(r===0&&Vu(e[0])){let l=t.segments[n];i.push(new nr(l.path,rD(e[0]))),r++;continue}let a=pl(o)?o.outlets[he]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&Vu(s)?(i.push(new nr(a,rD(s))),r+=2):(i.push(new nr(a,{})),r++)}return new Re(i,{})}function I1(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=h_(new Re([],{}),0,i))}),n}function rD(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function oD(t,n,e){return t==e.path&&Jn(n,e.parameters)}var ml="imperative",Dt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Dt||{}),rn=class{id;url;constructor(n,e){this.id=n,this.url=e}},oo=class extends rn{type=Dt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ei=class extends rn{urlAfterRedirects;type=Dt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},At=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(At||{}),gl=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(gl||{}),hn=class extends rn{reason;code;type=Dt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function ED(t){return t instanceof hn&&(t.code===At.Redirect||t.code===At.SupersededByNewNavigation)}var Si=class extends rn{reason;code;type=Dt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ao=class extends rn{error;target;type=Dt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},_l=class extends rn{urlAfterRedirects;state;type=Dt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},zu=class extends rn{urlAfterRedirects;state;type=Dt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Uu=class extends rn{urlAfterRedirects;state;shouldActivate;type=Dt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},$u=class extends rn{urlAfterRedirects;state;type=Dt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Gu=class extends rn{urlAfterRedirects;state;type=Dt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Wu=class{route;type=Dt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},qu=class{route;type=Dt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Qu=class{snapshot;type=Dt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Yu=class{snapshot;type=Dt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Ku=class{snapshot;type=Dt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Zu=class{snapshot;type=Dt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var la=class{},vl=class{},ca=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function S1(t){return!(t instanceof la)&&!(t instanceof ca)&&!(t instanceof vl)}var Xu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ha(this.rootInjector)}},ha=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Xu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(H(Le))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ju=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=p_(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=p_(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=g_(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return g_(n,this._root).map(e=>e.value)}};function p_(t,n){if(t===n.value)return n;for(let e of n.children){let i=p_(t,e);if(i)return i}return null}function g_(t,n){if(t===n.value)return[n];for(let e of n.children){let i=g_(t,e);if(i.length)return i.unshift(n),i}return[]}var tn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function oa(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var bl=class extends Ju{snapshot;constructor(n,e){super(n),this.snapshot=e,E_(this,n)}toString(){return this.snapshot.toString()}};function ID(t,n){let e=M1(t,n),i=new Xe([new nr("",{})]),r=new Xe({}),o=new Xe({}),a=new Xe({}),s=new Xe(""),l=new Mi(i,r,a,s,o,he,t,e.root);return l.snapshot=e.root,new bl(new tn(l,[]),e)}function M1(t,n){let e={},i={},r={},a=new da([],e,r,"",i,he,t,null,{},n);return new yl("",new tn(a,[]))}var Mi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(U(c=>c[Dl]))??Z(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(U(n=>ro(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(U(n=>ro(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function x_(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:b(b({},n.params),t.params),data:b(b({},n.data),t.data),resolve:b(b(b(b({},t.data),n.data),r?.data),t._resolvedData)}:i={params:b({},t.params),data:b({},t.data),resolve:b(b({},t.data),t._resolvedData??{})},r&&MD(r)&&(i.resolve[Dl]=r.title),i}var da=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Dl]}constructor(n,e,i,r,o,a,s,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=ro(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=ro(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},yl=class extends Ju{url;constructor(n,e){super(e),this.url=n,E_(this,e)}toString(){return SD(this._root)}};function E_(t,n){n.value._routerState=t,n.children.forEach(e=>E_(t,e))}function SD(t){let n=t.children.length>0?` { ${t.children.map(SD).join(", ")} } `:"";return`${t.value}${n}`}function l_(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Jn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Jn(n.params,e.params)||t.paramsSubject.next(e.params),n1(n.url,e.url)||t.urlSubject.next(e.url),Jn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function __(t,n){let e=Jn(t.params,n.params)&&s1(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||__(t.parent,n.parent))}function MD(t){return typeof t.title=="string"||t.title===null}var TD=new y(""),xl=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=he;activateEvents=new $;deactivateEvents=new $;attachEvents=new $;detachEvents=new $;routerOutletData=yC();parentContexts=d(ha);location=d(pt);changeDetector=d(ye);inputBinder=d(rf,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new S(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new S(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new S(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new S(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new v_(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ue]})}return t})(),v_=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Mi?this.route:n===ha?this.childContexts:n===TD?this.outletData:this.parent.get(n,e)}},rf=new y("");var I_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ie(0,"router-outlet")},dependencies:[xl],encapsulation:2})}return t})();function S_(t){let n=t.children&&t.children.map(S_),e=n?X(b({},t),{children:n}):b({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==he&&(e.component=I_),e}function T1(t,n,e){let i=wl(t,n._root,e?e._root:void 0);return new bl(i,n)}function wl(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=k1(t,n,e);return new tn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>wl(t,s)),a}}let i=R1(n.value),r=n.children.map(o=>wl(t,o));return new tn(i,r)}}function k1(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return wl(t,i,r);return wl(t,i)})}function R1(t){return new Mi(new Xe(t.url),new Xe(t.params),new Xe(t.queryParams),new Xe(t.fragment),new Xe(t.data),t.outlet,t.component,t)}var ua=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},kD="ngNavigationCancelingError";function ef(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=rr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=RD(!1,At.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function RD(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[kD]=!0,e.cancellationCode=n,e}function A1(t){return AD(t)&&rr(t.url)}function AD(t){return!!t&&t[kD]}var b_=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),l_(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=oa(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=oa(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=oa(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=oa(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Zu(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Yu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(l_(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),l_(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},tf=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},sa=class{component;route;constructor(n,e){this.component=n,this.route=e}};function O1(t,n,e){let i=t._root,r=n?n._root:null;return ul(i,r,e,[i.value])}function N1(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function pa(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!ih(t)?t:n.get(t):i}function ul(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=oa(n);return t.children.forEach(a=>{F1(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>hl(s,e.getContext(a),r)),r}function F1(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=P1(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new tf(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?ul(t,n,s?s.children:null,i,r):ul(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new sa(s.outlet.component,a))}else a&&hl(n,s,r),r.canActivateChecks.push(new tf(i)),o.component?ul(t,null,s?s.children:null,i,r):ul(t,null,e,i,r);return r}function P1(t,n,e){if(typeof e=="function")return ut(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!io(t.url,n.url);case"pathParamsOrQueryParamsChange":return!io(t.url,n.url)||!Jn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!__(t,n)||!Jn(t.queryParams,n.queryParams);default:return!__(t,n)}}function hl(t,n,e){let i=oa(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?hl(a,n.children.getContext(o),e):hl(a,null,e):hl(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new sa(n.outlet.component,r)):e.canDeactivateChecks.push(new sa(null,r)):e.canDeactivateChecks.push(new sa(null,r))}function El(t){return typeof t=="function"}function L1(t){return typeof t=="boolean"}function B1(t){return t&&El(t.canLoad)}function j1(t){return t&&El(t.canActivate)}function V1(t){return t&&El(t.canActivateChild)}function H1(t){return t&&El(t.canDeactivate)}function z1(t){return t&&El(t.canMatch)}function OD(t){return t instanceof Mr||t?.name==="EmptyError"}var Fu=Symbol("INITIAL_VALUE");function fa(){return Ie(t=>Tr(t.map(n=>n.pipe(Ce(1),Ge(Fu)))).pipe(U(n=>{for(let e of n)if(e!==!0){if(e===Fu)return Fu;if(e===!1||U1(e))return e}return!0}),ue(n=>n!==Fu),Ce(1)))}function U1(t){return rr(t)||t instanceof ua}function ND(t){return t.aborted?Z(void 0).pipe(Ce(1)):new re(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function FD(t){return fe(ND(t))}function $1(t){return Ft(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Z(X(b({},n),{guardsResult:!0})):G1(o,e,i).pipe(Ft(a=>a&&L1(a)?W1(e,r,t):Z(a)),U(a=>X(b({},n),{guardsResult:a})))})}function G1(t,n,e){return je(t).pipe(Ft(i=>Z1(i.component,i.route,e,n)),ui(i=>i!==!0,!0))}function W1(t,n,e){return je(n).pipe(ko(i=>Li(Q1(i.route.parent,e),q1(i.route,e),K1(t,i.path),Y1(t,i.route))),ui(i=>i!==!0,!0))}function q1(t,n){return t!==null&&n&&n(new Ku(t)),Z(!0)}function Q1(t,n){return t!==null&&n&&n(new Qu(t)),Z(!0)}function Y1(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Z(!0);let i=e.map(r=>wn(()=>{let o=n._environmentInjector,a=pa(r,o),s=j1(a)?a.canActivate(n,t):ut(o,()=>a(n,t));return so(s).pipe(ui())}));return Z(i).pipe(fa())}function K1(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>N1(o)).filter(o=>o!==null).map(o=>wn(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=pa(s,l),u=V1(c)?c.canActivateChild(e,t):ut(l,()=>c(e,t));return so(u).pipe(ui())});return Z(a).pipe(fa())}));return Z(r).pipe(fa())}function Z1(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Z(!0);let o=r.map(a=>{let s=n._environmentInjector,l=pa(a,s),c=H1(l)?l.canDeactivate(t,n,e,i):ut(s,()=>l(t,n,e,i));return so(c).pipe(ui())});return Z(o).pipe(fa())}function X1(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Z(!0);let a=o.map(s=>{let l=pa(s,t),c=B1(l)?l.canLoad(n,e):ut(t,()=>l(n,e)),u=so(c);return r?u.pipe(FD(r)):u});return Z(a).pipe(fa(),PD(i))}function PD(t){return Fm(et(n=>{if(typeof n!="boolean")throw ef(t,n)}),U(n=>n===!0))}function J1(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return Z(!0);let s=a.map(l=>{let c=pa(l,t),u=z1(c)?c.canMatch(n,e,r):ut(t,()=>c(n,e,r));return so(u).pipe(FD(o))});return Z(s).pipe(fa(),PD(i))}var Ii=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Cl=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function eN(t){throw new S(4e3,!1)}function tN(t){throw RD(!1,At.GuardRejected)}var y_=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[he])throw eN(`${n.redirectTo}`);r=r.children[he]}}async applyRedirectCommands(n,e,i,r,o){let a=await nN(e,r,o);if(a instanceof nn)throw new Cl(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new Cl(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new nn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new Re(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new S(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function nN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return ju(so(ut(e,()=>i(n))))}function iN(t,n){return t.providers&&!t._injector&&(t._injector=qs(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Nn(t){return t.outlet||he}function rN(t,n){let e=t.filter(i=>Nn(i)===n);return e.push(...t.filter(i=>Nn(i)!==n)),e}var w_={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function LD(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function oN(t,n,e,i,r,o,a){let s=BD(t,n,e);if(!s.matched)return Z(s);let l=LD(o(s));return i=iN(n,i),J1(i,n,e,r,l,a).pipe(U(c=>c===!0?s:b({},w_)))}function BD(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?b({},w_):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||lD)(e,t,n);if(!r)return b({},w_);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?b(b({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function aD(t,n,e,i,r){return e.length>0&&lN(t,e,i,r)?{segmentGroup:new Re(n,sN(i,new Re(e,t.children))),slicedSegments:[]}:e.length===0&&cN(t,e,i)?{segmentGroup:new Re(t.segments,aN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Re(t.segments,t.children),slicedSegments:e}}function aN(t,n,e,i){let r={};for(let o of e)if(of(t,n,o)&&!i[Nn(o)]){let a=new Re([],{});r[Nn(o)]=a}return b(b({},i),r)}function sN(t,n){let e={};e[he]=n;for(let i of t)if(i.path===""&&Nn(i)!==he){let r=new Re([],{});e[Nn(i)]=r}return e}function lN(t,n,e,i){return e.some(r=>!of(t,n,r)||!(Nn(r)!==he)?!1:!(i!==void 0&&Nn(r)===i))}function cN(t,n,e){return e.some(i=>of(t,n,i))}function of(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function dN(t,n,e){return n.length===0&&!t.children[e]}var C_=class{};async function uN(t,n,e,i,r,o,a="emptyOnly",s){return new D_(t,n,e,i,r,a,o,s).recognize()}var fN=31,D_=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new y_(this.urlSerializer,this.urlTree)}noMatchError(n){return new S(4002,`'${n.segmentGroup}'`)}async recognize(){let n=aD(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new tn(i,e),o=new yl("",r),a=yD(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new da([],Object.freeze({}),Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),he,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,he,e),rootSnapshot:e}}catch(i){if(i instanceof Cl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ii?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof tn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],u=rN(e,l),f=await this.processSegmentGroup(n,u,c,l,r);a.push(...f)}let s=jD(a);return mN(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof Ii||OD(c))continue;throw c}if(dN(i,r,o))return new C_;throw new Ii(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(Nn(i)!==a&&(a===he||!of(r,o,i)))throw new Ii(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new Ii(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:g}=BD(e,r,o);if(!l)throw new Ii(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>fN&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let C=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,LD(_),n),O=await this.applyRedirects.lineralizeSegments(r,C);return this.processSegment(n,i,e,O.concat(g),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new da(i,r,Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,pN(e),Nn(e),e.component??e._loadedComponent??null,e,gN(e),n),s=x_(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=_t=>this.createSnapshot(n,i,_t.consumedSegments,_t.parameters,a),l=await ju(oN(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Ii(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:g,remainingSegments:_}=l,C=this.createSnapshot(n,i,g,f,a),{segmentGroup:O,slicedSegments:L}=aD(e,g,_,c,o);if(L.length===0&&O.hasChildren()){let _t=await this.processChildren(u,c,O,C);return new tn(C,_t)}if(c.length===0&&L.length===0)return new tn(C,[]);let Y=Nn(i)===o,Me=await this.processSegment(u,c,O,L,Y?he:o,!0,C);return new tn(C,Me instanceof tn?[Me]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await ju(X1(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw tN(e)}return{routes:[],injector:n}}};function mN(t){t.sort((n,e)=>n.value.outlet===he?-1:e.value.outlet===he?1:n.value.outlet.localeCompare(e.value.outlet))}function hN(t){let n=t.value.routeConfig;return n&&n.path===""}function jD(t){let n=[],e=new Set;for(let i of t){if(!hN(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=jD(i.children);n.push(new tn(i.value,r))}return n.filter(i=>!e.has(i))}function pN(t){return t.data||{}}function gN(t){return t.resolve||{}}function _N(t,n,e,i,r,o,a){return Ft(async s=>{let{state:l,tree:c}=await uN(t,n,e,i,s.extractedUrl,r,o,a);return X(b({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function vN(t){return Ft(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Z(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of VD(s))o.add(l);let a=0;return je(o).pipe(ko(s=>r.has(s)?bN(s,e,t):(s.data=x_(s,s.parent,t).resolve,Z(void 0))),et(()=>a++),$c(1),Ft(s=>a===o.size?Z(n):Je))})}function VD(t){let n=t.children.map(e=>VD(e)).flat();return[t,...n]}function bN(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!MD(i)&&(r[Dl]=i.title),wn(()=>(t.data=x_(t,t.parent,e).resolve,yN(r,t,n).pipe(U(o=>(t._resolvedData=o,t.data=b(b({},t.data),o),null)))))}function yN(t,n,e){let i=d_(t);if(i.length===0)return Z({});let r={};return je(i).pipe(Ft(o=>wN(t[o],n,e).pipe(ui(),et(a=>{if(a instanceof ua)throw ef(new ir,a);r[o]=a}))),$c(1),U(()=>r),$n(o=>OD(o)?Je:ss(o)))}function wN(t,n,e){let i=n._environmentInjector,r=pa(t,i),o=r.resolve?r.resolve(n,e):ut(i,()=>r(n,e));return so(o)}function sD(t){return Ie(n=>{let e=t(n);return e?je(e).pipe(U(()=>n)):Z(n)})}var M_=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===he);return i}getResolvedTitleForRoute(e){return e.data[Dl]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(HD),providedIn:"root"})}return t})(),HD=(()=>{class t extends M_{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(H(JC))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ga=new y("",{factory:()=>({})}),Il=new y(""),zD=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(Ig);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await dD(ut(e,()=>i.loadComponent())),a=await GD($D(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await UD(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function UD(t,n,e,i){let r=await dD(ut(e,()=>t.loadChildren())),o=await GD($D(r)),a;o instanceof lu||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,u;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,u=a,l=s.get(Il,[],{optional:!0,self:!0}).flat()),{routes:l.map(S_),injector:s,factory:u}}function CN(t){return t&&typeof t=="object"&&"default"in t}function $D(t){return CN(t)?t.default:t}async function GD(t){return t}var af=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(DN),providedIn:"root"})}return t})(),DN=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),WD=new y("");var xN=()=>{},qD=new y(""),QD=(()=>{class t{currentNavigation=le(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=le(null);events=new x;transitionAbortWithErrorSubject=new x;configLoader=d(zD);environmentInjector=d(Le);destroyRef=d(mt);urlSerializer=d(ma);rootContexts=d(ha);location=d(er);inputBindingEnabled=d(rf,{optional:!0})!==null;titleStrategy=d(M_);options=d(ga,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(af);createViewTransition=d(WD,{optional:!0});navigationErrorHandler=d(qD,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Z(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Wu(r)),i=r=>this.events.next(new qu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;ke(()=>{this.transitions?.next(X(b({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Xe(null),this.transitions.pipe(ue(i=>i!==null),Ie(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return Z(i).pipe(Ie(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",At.SupersededByNewNavigation),Je;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?X(b({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new Si(s.id,this.urlSerializer.serialize(s.rawUrl),"",gl.IgnoredSameUrlNavigation)),s.resolve(!1),Je;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return Z(s).pipe(Ie(f=>(this.events.next(new oo(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?Je:Promise.resolve(f))),_N(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),et(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=f.urlAfterRedirects,g)),this.events.next(new vl)}),Ie(f=>je(i.routesRecognizeHandler.deferredHandle??Z(void 0)).pipe(U(()=>f))),et(()=>{let f=new _l(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:f,extractedUrl:g,source:_,restoredState:C,extras:O}=s,L=new oo(f,this.urlSerializer.serialize(g),_,C);this.events.next(L);let Y=ID(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=X(b({},s),{targetSnapshot:Y,urlAfterRedirects:g,extras:X(b({},O),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Me=>(Me.finalUrl=g,Me)),Z(i)}else return this.events.next(new Si(s.id,this.urlSerializer.serialize(s.extractedUrl),"",gl.IgnoredByUrlHandlingStrategy)),s.resolve(!1),Je}),U(s=>{let l=new zu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=X(b({},s),{guards:O1(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),$1(s=>this.events.next(s)),Ie(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw ef(this.urlSerializer,s.guardsResult);let l=new Uu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return Je;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",At.GuardRejected),Je;if(s.guards.canActivateChecks.length===0)return Z(s);let c=new $u(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return Je;let u=!1;return Z(s).pipe(vN(this.paramsInheritanceStrategy),et({next:()=>{u=!0;let f=new Gu(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)},complete:()=>{u||this.cancelNavigationTransition(s,"",At.NoDataFromResolver)}}))}),sD(s=>{let l=u=>{let f=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let g=u._environmentInjector;f.push(this.configLoader.loadComponent(g,u.routeConfig).then(_=>{u.component=_}))}for(let g of u.children)f.push(...l(g));return f},c=l(s.targetSnapshot.root);return c.length===0?Z(s):je(Promise.all(c).then(()=>s))}),sD(()=>this.afterPreactivation()),Ie(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?je(c).pipe(U(()=>i)):Z(i)}),Ce(1),Ie(s=>{let l=T1(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=X(b({},s),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new la);let c=i.beforeActivateHandler.deferredHandle;return c?je(c.then(()=>s)):Z(s)}),et(s=>{new b_(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=xN,l)),this.lastSuccessfulNavigation.set(ke(this.currentNavigation)),this.events.next(new ei(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),fe(ND(o.signal).pipe(ue(()=>!r&&!i.targetRouterState),et(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",At.Aborted)}))),et({complete:()=>{r=!0}}),fe(this.transitionAbortWithErrorSubject.pipe(et(s=>{throw s}))),Bi(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",At.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),$n(s=>{if(r=!0,this.destroyed)return i.resolve(!1),Je;if(AD(s))this.events.next(new hn(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),A1(s)?this.events.next(new ca(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ao(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=ut(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof ua){let{message:u,cancellationCode:f}=ef(this.urlSerializer,c);this.events.next(new hn(i.id,this.urlSerializer.serialize(i.extractedUrl),u,f)),this.events.next(new ca(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return Je}))}))}cancelNavigationTransition(e,i,r){let o=new hn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=ke(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function EN(t){return t!==ml}var YD=new y("");var KD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(IN),providedIn:"root"})}return t})(),nf=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},IN=(()=>{class t extends nf{static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),sf=(()=>{class t{urlSerializer=d(ma);options=d(ga,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(er);urlHandlingStrategy=d(af);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new nn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof nn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=ID(null,d(Le));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(SN),providedIn:"root"})}return t})(),SN=(()=>{class t extends sf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof oo?this.updateStateMemento():e instanceof Si?this.commitTransition(i):e instanceof _l?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof la?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof hn&&!ED(e)?this.restoreHistory(i):e instanceof ao?this.restoreHistory(i,!0):e instanceof ei&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=b(b({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=b(b({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?b({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):b({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function T_(t,n){t.events.pipe(ue(e=>e instanceof ei||e instanceof hn||e instanceof ao||e instanceof Si),U(e=>e instanceof ei||e instanceof Si?0:(e instanceof hn?e.code===At.Redirect||e.code===At.SupersededByNewNavigation:!1)?2:1),ue(e=>e!==2),Ce(1)).subscribe(()=>{n()})}var _a=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(du);stateManager=d(sf);options=d(ga,{optional:!0})||{};pendingTasks=d(vi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(QD);urlSerializer=d(ma);location=d(er);urlHandlingStrategy=d(af);injector=d(Le);_events=new x;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(KD);injectorCleanup=d(YD,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Il,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(rf,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new be;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=ke(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof hn&&i.code!==At.Redirect&&i.code!==At.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ei)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof ca){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=b({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||EN(r.source)},a);this.scheduleNavigation(s,ml,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}S1(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),ml,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=X(b({},o),{browserUrl:e})),r){let c=b({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(cn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return ke(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(S_),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,u=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":u=b(b({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let g=r?r.snapshot:this.routerState.snapshot.root;f=wD(g)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return CD(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=rr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,ml,null,i)}navigate(e,i={skipLocationChange:!1}){return MN(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Cn(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=b({},fD):i===!1?r=b({},u_):r=b(b({},u_),i),rr(e))return tD(this.currentUrlTree,e,r);let o=this.parseUrl(e);return tD(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((f,g)=>{s=f,l=g});let u=this.pendingTasks.add();return T_(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function MN(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new S(4008,!1)}var RN=(()=>{class t{router=d(_a);stateManager=d(sf);fragment=le("");queryParams=le({});path=le("");serializer=d(ma);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof ei&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new nn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),va=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new fn("href"),{optional:!0});reactiveHref=Sg(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return ke(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return ke(this._target)}_target=le(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return ke(this._queryParams)}_queryParams=le(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return ke(this._fragment)}_fragment=le(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return ke(this._queryParamsHandling)}_queryParamsHandling=le(void 0);set state(e){this._state.set(e)}get state(){return ke(this._state)}_state=le(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return ke(this._info)}_info=le(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return ke(this._relativeTo)}_relativeTo=le(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return ke(this._preserveFragment)}_preserveFragment=le(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return ke(this._skipLocationChange)}_skipLocationChange=le(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return ke(this._replaceUrl)}_replaceUrl=le(!1);isAnchorElement;onChanges=new x;applicationErrorHandler=d(cn);options=d(ga,{optional:!0});reactiveRouterState=d(RN);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=le(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(rr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Ht(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:rr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return ke(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(_e(_a),_e(Mi),js("tabindex"),_e(Te),_e(F),_e(ea))};static \u0275dir=N({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&W("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&J("href",r.reactiveHref(),Kp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",ee],skipLocationChange:[2,"skipLocationChange","skipLocationChange",ee],replaceUrl:[2,"replaceUrl","replaceUrl",ee],routerLink:"routerLink"},features:[Ue]})}return t})();var AN=new y("");function k_(t,...n){return xt([{provide:Il,multi:!0,useValue:t},[],{provide:Mi,useFactory:ON},{provide:mu,multi:!0,useFactory:NN},n.map(e=>e.\u0275providers)])}function ON(){return d(_a).routerState.root}function NN(){let t=d(B);return n=>{let e=t.get(Et);if(n!==e.components[0])return;let i=t.get(_a),r=t.get(FN);t.get(PN)===1&&i.initialNavigation(),t.get(LN,null,{optional:!0})?.setUpPreloading(),t.get(AN,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var FN=new y("",{factory:()=>new x}),PN=new y("",{factory:()=>1});var LN=new y("");var R_="Service workers are disabled or not supported by this browser",ba=class{serviceWorker;worker;registration;events;constructor(n,e){if(this.serviceWorker=n,!n)this.worker=this.events=this.registration=new re(i=>i.error(new S(5601,!1)));else{let i=null,r=new x;this.worker=new re(c=>(i!==null&&c.next(i),r.subscribe(u=>c.next(u))));let o=()=>{let{controller:c}=n;c!==null&&(i=c,r.next(i))};n.addEventListener("controllerchange",o),o(),this.registration=this.worker.pipe(Ie(()=>n.getRegistration().then(c=>{if(!c)throw new S(5601,!1);return c})));let a=new x;this.events=a.asObservable();let s=c=>{let{data:u}=c;u?.type&&a.next(u)};n.addEventListener("message",s),e?.get(Et,null,{optional:!0})?.onDestroy(()=>{n.removeEventListener("controllerchange",o),n.removeEventListener("message",s)})}}postMessage(n,e){return new Promise(i=>{this.worker.pipe(Ce(1)).subscribe(r=>{r.postMessage(b({action:n},e)),i()})})}postMessageWithOperation(n,e,i){let r=this.waitForOperationCompleted(i),o=this.postMessage(n,e);return Promise.all([o,r]).then(([,a])=>a)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(n){let e;return typeof n=="string"?e=i=>i.type===n:e=i=>n.includes(i.type),this.events.pipe(ue(e))}nextEventOfType(n){return this.eventsOfType(n).pipe(Ce(1))}waitForOperationCompleted(n){return new Promise((e,i)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(ue(r=>r.nonce===n),Ce(1),U(r=>{if(r.result!==void 0)return r.result;throw new Error(r.error)})).subscribe({next:e,error:i})})}get isEnabled(){return!!this.serviceWorker}},XD=(()=>{class t{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new x;constructor(e){if(this.sw=e,!e.isEnabled){this.messages=di,this.notificationClicks=di,this.notificationCloses=di,this.pushSubscriptionChanges=di,this.subscription=di;return}this.messages=this.sw.eventsOfType("PUSH").pipe(U(r=>r.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(U(r=>r.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(U(r=>r.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(U(r=>r.data)),this.pushManager=this.sw.registration.pipe(U(r=>r.pushManager));let i=this.pushManager.pipe(Ie(r=>r.getSubscription()));this.subscription=new re(r=>{let o=i.subscribe(r),a=this.subscriptionChanges.subscribe(r);return()=>{o.unsubscribe(),a.unsubscribe()}})}requestSubscription(e){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(R_));let i={userVisibleOnly:!0},r=this.decodeBase64(e.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),o=new Uint8Array(new ArrayBuffer(r.length));for(let a=0;a<r.length;a++)o[a]=r.charCodeAt(a);return i.applicationServerKey=o,new Promise((a,s)=>{this.pushManager.pipe(Ie(l=>l.subscribe(i)),Ce(1)).subscribe({next:l=>{this.subscriptionChanges.next(l),a(l)},error:s})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(R_));let e=i=>{if(i===null)throw new S(5602,!1);return i.unsubscribe().then(r=>{if(!r)throw new S(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((i,r)=>{this.subscription.pipe(Ce(1),Ie(e)).subscribe({next:i,error:r})})}decodeBase64(e){return atob(e)}static \u0275fac=function(i){return new(i||t)(H(ba))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),lf=(()=>{class t{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(e){if(this.sw=e,!e.isEnabled){this.versionUpdates=di,this.unrecoverable=di;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(R_));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let e=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:e},e).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new S(5601,!1));let e=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:e},e)}static \u0275fac=function(i){return new(i||t)(H(ba))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),JD=new y("");function BN(){let t=d(Sl);if(!("serviceWorker"in navigator&&t.enabled!==!1))return;let n=d(JD),e=d(G),i=d(Et);e.runOutsideAngular(()=>{let r=navigator.serviceWorker,o=()=>r.controller?.postMessage({action:"INITIALIZE"});r.addEventListener("controllerchange",o),i.onDestroy(()=>{r.removeEventListener("controllerchange",o)})}),e.runOutsideAngular(()=>{let r,{registrationStrategy:o}=t;if(typeof o=="function")r=new Promise(a=>o().subscribe(()=>a()));else{let[a,...s]=(o||"registerWhenStable:30000").split(":");switch(a){case"registerImmediately":r=Promise.resolve();break;case"registerWithDelay":r=ZD(+s[0]||0);break;case"registerWhenStable":r=Promise.race([i.whenStable(),ZD(+s[0])]);break;default:throw new S(5600,!1)}}r.then(()=>{i.destroyed||navigator.serviceWorker.register(n,{scope:t.scope,updateViaCache:t.updateViaCache,type:t.type}).catch(a=>console.error(Cn(5604,!1)))})})}function ZD(t){return new Promise(n=>setTimeout(n,t))}function jN(){let t=d(Sl),n=d(B),e=!0;return new ba(e&&t.enabled!==!1?navigator.serviceWorker:void 0,n)}var Sl=class{enabled;updateViaCache;type;scope;registrationStrategy};function VN(t,n={}){return xt([XD,lf,{provide:JD,useValue:t},{provide:Sl,useValue:n},{provide:ba,useFactory:jN},fu(BN)])}var ex=(()=>{class t{static register(e,i={}){return{ngModule:t,providers:[VN(e,i)]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({providers:[XD,lf]})}return t})();function lo(t){return t.buttons===0||t.detail===0}function co(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var A_;function tx(){if(A_==null){let t=typeof document<"u"?document.head:null;A_=!!(t&&(t.createShadowRoot||t.attachShadow))}return A_}function O_(t){if(tx()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Ml(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Tt(t){return t.composedPath?t.composedPath()[0]:t.target}var N_;try{N_=typeof Intl<"u"&&Intl.v8BreakIterator}catch{N_=!1}var we=(()=>{class t{_platformId=d(Xr);isBrowser=this._platformId?RC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||N_)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Tl;function nx(){if(Tl==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Tl=!0}))}finally{Tl=Tl||!1}return Tl}function ya(t){return nx()?t:!!t.capture}function Ut(t,n=0){return ix(t)?Number(t):arguments.length===2?n:0}function ix(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function ti(t){return t instanceof F?t.nativeElement:t}var rx=new y("cdk-input-modality-detector-options"),ox={ignoreKeys:[18,17,224,91,16]},ax=650,F_={passive:!0,capture:!0},sx=(()=>{class t{_platform=d(we);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Xe(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Tt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<ax||(this._modality.next(lo(e)?"keyboard":"mouse"),this._mostRecentTarget=Tt(e))};_onTouchstart=e=>{if(co(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Tt(e)};constructor(){let e=d(G),i=d(K),r=d(rx,{optional:!0});if(this._options=b(b({},ox),r),this.modalityDetected=this._modality.pipe(ds(1)),this.modalityChanged=this.modalityDetected.pipe(Uc()),this._platform.isBrowser){let o=d(bt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,F_),o.listen(i,"mousedown",this._onMousedown,F_),o.listen(i,"touchstart",this._onTouchstart,F_)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),kl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(kl||{}),lx=new y("cdk-focus-monitor-default-options"),cf=ya({passive:!0,capture:!0}),Ot=(()=>{class t{_ngZone=d(G);_platform=d(we);_inputModalityDetector=d(sx);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(K);_stopInputModalityDetector=new x;constructor(){let e=d(lx,{optional:!0});this._detectionMode=e?.detectionMode||kl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Tt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=ti(e);if(!this._platform.isBrowser||r.nodeType!==1)return Z();let o=O_(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new x,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=ti(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=ti(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===kl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===kl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?ax:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Tt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,cf),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,cf)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(fe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,cf),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,cf),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var df=new WeakMap,st=(()=>{class t{_appRef;_injector=d(B);_environmentInjector=d(Le);load(e){let i=this._appRef=this._appRef||this._injector.get(Et),r=df.get(i);r||(r={loaders:new Set,refs:[]},df.set(i,r),i.onDestroy(()=>{df.get(i)?.refs.forEach(o=>o.destroy()),df.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Cu(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2,changeDetection:0})}return t})(),uf;function HN(){if(uf===void 0&&(uf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(uf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return uf}function uo(t){return HN()?.createHTML(t)||t}function cx(t,n,e){let i=e.sanitize(ot.HTML,n);t.innerHTML=uo(i||"")}function Ca(t){return Array.isArray(t)?t:[t]}var dx=new Set,fo,Da=(()=>{class t{_platform=d(we);_nonce=d(Jr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):UN}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&zN(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function zN(t,n){if(!dx.has(t))try{fo||(fo=document.createElement("style"),n&&fo.setAttribute("nonce",n),fo.setAttribute("type","text/css"),document.head.appendChild(fo)),fo.sheet&&(fo.sheet.insertRule(`@media ${t} {body{ }}`,0),dx.add(t))}catch(e){console.error(e)}}function UN(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Fn=(()=>{class t{_mediaMatcher=d(Da);_zone=d(G);_queries=new Map;_destroySubject=new x;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return ux(Ca(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=ux(Ca(e)).map(a=>this._registerQuery(a).observable),o=Tr(r);return o=Li(o.pipe(Ce(1)),o.pipe(ds(1),kr(0))),o.pipe(U(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new re(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(Ge(i),U(({matches:a})=>({query:e,matches:a})),fe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ux(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var $N=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var xa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({providers:[$N]})}return t})();var Ea=(()=>{class t{_platform=d(we);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return WN(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=GN(eF(e));if(i&&(fx(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=fx(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!XN(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return JN(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function GN(t){try{return t.frameElement}catch{return null}}function WN(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function qN(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function QN(t){return KN(t)&&t.type=="hidden"}function YN(t){return ZN(t)&&t.hasAttribute("href")}function KN(t){return t.nodeName.toLowerCase()=="input"}function ZN(t){return t.nodeName.toLowerCase()=="a"}function px(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function fx(t){if(!px(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function XN(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function JN(t){return QN(t)?!1:qN(t)||YN(t)||t.hasAttribute("contenteditable")||px(t)}function eF(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var ff=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?$e(n,{injector:this._injector}):setTimeout(n)}},Rl=(()=>{class t{_checker=d(Ea);_ngZone=d(G);_document=d(K);_injector=d(B);constructor(){d(st).load(wa)}create(e,i=!1){return new ff(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var gx=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),_x=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),tF=0,Al=(()=>{class t{_ngZone=d(G);_defaultOptions=d(_x,{optional:!0});_liveElement;_document=d(K);_sanitizer=d(cl);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(gx,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:cx(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${tF++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var or=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(or||{}),mx="cdk-high-contrast-black-on-white",hx="cdk-high-contrast-white-on-black",P_="cdk-high-contrast-active",vx=(()=>{class t{_platform=d(we);_hasCheckedHighContrastMode=!1;_document=d(K);_breakpointSubscription;constructor(){this._breakpointSubscription=d(Fn).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return or.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return or.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return or.BLACK_ON_WHITE}return or.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(P_,mx,hx),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===or.BLACK_ON_WHITE?e.add(P_,mx):i===or.WHITE_ON_BLACK&&e.add(P_,hx)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ol=(()=>{class t{constructor(){d(vx)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[xa]})}return t})();var nF=200,mf=class{_letterKeyStream=new x;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new x;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:nF;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(et(e=>this._pressedLetters.push(e)),kr(n),ue(()=>this._pressedLetters.length>0),U(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function lt(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Ia=class{_items;_activeItemIndex=le(-1);_activeItem=le(null);_wrap=!1;_typeaheadSubscription=be.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof kn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):wi(n)&&(this._effectRef=qi(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new x;change=new x;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new mf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||lt(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return wi(this._items)?this._items():this._items instanceof kn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Bl=class extends Ia{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var jl=class extends Ia{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var L_={},He=class t{_appId=d(Yi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),L_.hasOwnProperty(n)||(L_[n]=0),`${n}${e?t._infix+"-":""}${L_[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var wx=" ";function V_(t,n,e){let i=pf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(wx)))}function gf(t,n,e){let i=pf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(wx)):t.removeAttribute(n)}function pf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var Cx="cdk-describedby-message",hf="cdk-describedby-host",j_=0,Dx=(()=>{class t{_platform=d(we);_document=d(K);_messageRegistry=new Map;_messagesContainer=null;_id=`${j_++}`;constructor(){d(st).load(wa),this._id=d(Yi)+"-"+j_++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=B_(i,r);typeof i!="string"?(yx(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=B_(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${hf}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(hf);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");yx(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(B_(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=pf(e,"aria-describedby").filter(r=>r.indexOf(Cx)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);V_(e,"aria-describedby",r.messageElement.id),e.setAttribute(hf,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,gf(e,"aria-describedby",r.messageElement.id),e.removeAttribute(hf)}_isElementDescribedByMessage(e,i){let r=pf(e,"aria-describedby"),o=this._messageRegistry.get(i),a=o&&o.messageElement.id;return!!a&&r.indexOf(a)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function B_(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function yx(t,n){t.id||(t.id=`${Cx}-${n}-${j_++}`)}var ar={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};function H_(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function rt(t){return t==null?"":typeof t=="string"?t:`${t}px`}var iF=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(K)}),rF=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function xx(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?rF.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Ke=(()=>{class t{get value(){return this.valueSignal()}valueSignal=le("ltr");change=new $;constructor(){let e=d(iF,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(xx(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Pn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Pn||{}),_f,mo;function vf(){if(mo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return mo=!1,mo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)mo=!0;else{let t=Element.prototype.scrollTo;t?mo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):mo=!1}}return mo}function Sa(){if(typeof document!="object"||!document)return Pn.NORMAL;if(_f==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),_f=Pn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,_f=t.scrollLeft===0?Pn.NEGATED:Pn.INVERTED),t.remove()}return _f}var ae=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();var oF=20,ni=(()=>{class t{_ngZone=d(G);_platform=d(we);_renderer=d(bt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new x;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=oF){return this._platform.isBrowser?new re(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Hc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Z()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ue(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=ti(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ti=(()=>{class t{elementRef=d(F);scrollDispatcher=d(ni);ngZone=d(G);dir=d(Ke,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new x;_renderer=d(Te);_cleanupScroll;_elementScrolled=new x;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&Sa()!=Pn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),Sa()==Pn.INVERTED?e.left=e.right:Sa()==Pn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;vf()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&Sa()==Pn.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&Sa()==Pn.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),aF=20,gn=(()=>{class t{_platform=d(we);_listeners;_viewportSize=null;_change=new x;_document=d(K);constructor(){let e=d(G),i=d(bt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=aF){return e>0?this._change.pipe(Hc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var pn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})(),Vl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae,pn,ae,pn]})}return t})();var Hl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Ln=class extends Hl{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Bn=class extends Hl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},z_=class extends Hl{element;constructor(n){super(),this.element=n instanceof F?n.nativeElement:n}},sr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Ln)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Bn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof z_)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},zl=class extends sr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Zn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||B.NULL,o=r.get(Le,i.injector);e=Cu(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var lr=(()=>{class t extends sr{_moduleRef=d(Zn,{optional:!0});_document=d(K);_viewContainerRef=d(pt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new $;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[ve]})}return t})(),cr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();var Ex=vf();function ka(t){return new bf(t.get(gn),t.get(K))}var bf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=rt(-this._previousScrollPosition.left),n.style.top=rt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),Ex&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),Ex&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function Ax(t,n){return new yf(t.get(ni),t.get(G),t.get(gn),n)}var yf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ue(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Ul=class{enable(){}disable(){}attach(){}};function U_(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function Ix(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function ki(t,n){return new wf(t.get(ni),t.get(gn),t.get(G),n)}var wf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();U_(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},Ox=(()=>{class t{_injector=d(B);constructor(){}noop=()=>new Ul;close=e=>Ax(this._injector,e);block=()=>ka(this._injector);reposition=e=>ki(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),jn=class{positionStrategy;scrollStrategy=new Ul;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Cf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var Nx=(()=>{class t{_attachedOverlays=[];_document=d(K);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Fx=(()=>{class t extends Nx{_ngZone=d(G);_renderer=d(bt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Px=(()=>{class t extends Nx{_platform=d(we);_ngZone=d(G);_renderer=d(bt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Tt(e)};_clickListener=e=>{let i=Tt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(Sx(s.overlayElement,i)||Sx(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Sx(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var Lx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2,changeDetection:0})}return t})(),Ef=(()=>{class t{_platform=d(we);_containerElement;_document=d(K);_styleLoader=d(st);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||H_()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),H_()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(Lx)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$_=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function G_(t){return t&&t.nodeType===1}var Ma=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new x;_attachments=new x;_detachments=new x;_positionStrategy;_scrollStrategy;_locationChanges=be.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new x;_outsidePointerEvents=new x;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,u=!1,f,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=g,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=$e(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=b(b({},this._config),n),this._updateElementSize()}setDirection(n){this._config=X(b({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=rt(this._config.width),n.height=rt(this._config.height),n.minWidth=rt(this._config.minWidth),n.minHeight=rt(this._config.minHeight),n.maxWidth=rt(this._config.maxWidth),n.maxHeight=rt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;G_(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new $_(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Ca(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=$e(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},Mx="cdk-overlay-connected-position-bounding-box",lF=/([A-Za-z%]+)$/;function po(t,n){return new Df(n,t.get(gn),t.get(K),t.get(we),t.get(Ef))}var Df=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new x;_resizeSubscription=be.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(Mx),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),u=this._getOverlayFit(c,e,i,s);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<u.visibleArea)&&(a={overlayFit:u,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&ho(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(Mx),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof F?this._origin.nativeElement:G_(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=kx(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let u=0-a,f=a+o.width-i.width,g=0-s,_=s+o.height-i.height,C=this._subtractOverflows(o.width,u,f),O=this._subtractOverflows(o.height,g,_),L=C*O;return{visibleArea:L,isCompletelyWithinViewport:o.width*o.height===L,fitsInViewportVertically:O===o.height,fitsInViewportHorizontally:C==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=Tx(this._overlayRef.getConfig().minHeight),s=Tx(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=kx(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-a:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-s:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!cF(this._lastScrollVisibility,i)){let r=new Cf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),C=this._lastBoundingBoxSize.height;o=_*2,a=n.y-_,o>C&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-C/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,g;if(c)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),C=this._lastBoundingBoxSize.width;u=_*2,f=n.x-_,u>C&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-C/2)}return{top:a,left:f,bottom:s,right:g,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=rt(i.width),r.height=rt(i.height),r.top=rt(i.top)||"auto",r.bottom=rt(i.bottom)||"auto",r.left=rt(i.left)||"auto",r.right=rt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=rt(o)),a&&(r.maxWidth=rt(a))}this._lastBoundingBoxSize=i,ho(this._boundingBox.style,r)}_resetBoundingBoxStyles(){ho(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){ho(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();ho(i,this._getExactOverlayY(e,n,u)),ho(i,this._getExactOverlayX(e,n,u))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=rt(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=rt(a.maxWidth):o&&(i.maxWidth="")),ho(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=rt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=rt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:Ix(n,i),isOriginOutsideView:U_(n,i),isOverlayClipped:Ix(e,i),isOverlayOutsideView:U_(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Ca(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof F)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function ho(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function Tx(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(lF);return!e||e==="px"?parseFloat(n):null}return t||null}function kx(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function cF(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var Rx="cdk-global-overlay-wrapper";function dr(t){return new xf}var xf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(Rx),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),u=this._xPosition,f=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",_="",C="",O="";l?O="flex-start":u==="center"?(O="center",g?C=f:_=f):g?u==="left"||u==="end"?(O="flex-end",_=f):(u==="right"||u==="start")&&(O="flex-start",C=f):u==="left"||u==="start"?(O="flex-start",_=f):(u==="right"||u==="end")&&(O="flex-end",C=f),n.position=this._cssPosition,n.marginLeft=l?"0":_,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":C,e.justifyContent=O,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(Rx),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},Bx=(()=>{class t{_injector=d(B);constructor(){}global(){return dr()}flexibleConnectedTo(e){return po(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$l=new y("OVERLAY_DEFAULT_CONFIG");function Hn(t,n){t.get(st).load(Lx);let e=t.get(Ef),i=t.get(K),r=t.get(He),o=t.get(Et),a=t.get(Ke),s=t.get(Te,null,{optional:!0})||t.get(bt).createRenderer(null,null),l=new jn(n),c=t.get($l,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let g=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return G_(g)?g.after(f):g?.type==="parent"?g.element.appendChild(f):e.getContainerElement().appendChild(f),new Ma(new zl(u,o,t),f,u,l,t.get(G),t.get(Fx),i,t.get(er),t.get(Px),n?.disableAnimations??t.get(Vs,null,{optional:!0})==="NoopAnimations",t.get(Le),s)}var jx=(()=>{class t{scrollStrategies=d(Ox);_positionBuilder=d(Bx);_injector=d(B);constructor(){}create(e){return Hn(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),dF=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],uF=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ki(t)}}),Ta=(()=>{class t{elementRef=d(F);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),Vx=new y("cdk-connected-overlay-default-config"),If=(()=>{class t{_dir=d(Ke,{optional:!0});_injector=d(B);_overlayRef;_templatePortal;_backdropSubscription=be.EMPTY;_attachSubscription=be.EMPTY;_detachSubscription=be.EMPTY;_positionSubscription=be.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(uF);_ngZone=d(G);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new $;positionChange=new $;attach=new $;detach=new $;overlayKeydown=new $;overlayOutsideClick=new $;constructor(){let e=d(ht),i=d(pt),r=d(Vx,{optional:!0}),o=d($l,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Bn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=dF);let e=this._overlayRef=Hn(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!lt(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Tt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new jn({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=po(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ta?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ta?this.origin.elementRef.nativeElement:this.origin instanceof F?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Hm(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",ee],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",ee],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",ee],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",ee],push:[2,"cdkConnectedOverlayPush","push",ee],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",ee],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",ee],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ue]})}return t})(),_n=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({providers:[jx],imports:[ae,cr,Vl,Vl]})}return t})();var fF=new y("MATERIAL_ANIMATIONS"),Hx=null;function mF(){return d(fF,{optional:!0})?.animationsDisabled||d(Vs,{optional:!0})==="NoopAnimations"?"di-disabled":(Hx??=d(Da).matchMedia("(prefers-reduced-motion)").matches,Hx?"reduced-motion":"enabled")}function Fe(){return mF()!=="enabled"}function $t(t){return t!=null&&`${t}`!="false"}var vn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(vn||{}),W_=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=vn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},zx=ya({passive:!0,capture:!0}),q_=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,zx)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,zx)))}_delegateEventHandler=n=>{let e=Tt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Gl={enterDuration:225,exitDuration:150},hF=800,Ux=ya({passive:!0,capture:!0}),$x=["mousedown","touchstart"],Gx=["mouseup","mouseleave","touchend","touchcancel"],pF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return t})(),Wl=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new q_;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=ti(i)),o&&o.get(st).load(pF)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=b(b({},Gl),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||gF(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${s-a}px`,u.style.top=`${l-a}px`,u.style.height=`${a*2}px`,u.style.width=`${a*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),g=f.transitionProperty,_=f.transitionDuration,C=g==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,O=new W_(this,u,i,C);u.style.transform="scale3d(1, 1, 1)",O.state=vn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=O);let L=null;return!C&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let Y=()=>{L&&(L.fallbackTimer=null),clearTimeout(_t),this._finishRippleTransition(O)},Me=()=>this._destroyRipple(O),_t=setTimeout(Me,c+100);u.addEventListener("transitionend",Y),u.addEventListener("transitioncancel",Me),L={onTransitionEnd:Y,onTransitionCancel:Me,fallbackTimer:_t}}),this._activeRipples.set(O,L),(C||!c)&&this._finishRippleTransition(O),O}fadeOutRipple(n){if(n.state===vn.FADING_OUT||n.state===vn.HIDDEN)return;let e=n.element,i=b(b({},Gl),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=vn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=ti(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,$x.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Gx.forEach(e=>{this._triggerElement.addEventListener(e,this,Ux)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===vn.FADING_IN?this._startFadeOutTransition(n):n.state===vn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=vn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=vn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=lo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+hF;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!co(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===vn.VISIBLE||n.config.terminateOnPointerUp&&n.state===vn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&($x.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(Gx.forEach(e=>n.removeEventListener(e,this,Ux)),this._pointerUpEventsRegistered=!1))}};function gF(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Q_=new y("mat-ripple-global-options"),ur=(()=>{class t{_elementRef=d(F);_animationsDisabled=Fe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(G),i=d(we),r=d(Q_,{optional:!0}),o=d(B);this._globalOptions=r||{},this._rippleRenderer=new Wl(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:b(b(b({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,b(b({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,b(b({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var _F={capture:!0},vF=["focus","mousedown","mouseenter","touchstart"],Y_="mat-ripple-loader-uninitialized",K_="mat-ripple-loader-class-name",Wx="mat-ripple-loader-centered",Sf="mat-ripple-loader-disabled",qx=(()=>{class t{_document=d(K);_animationsDisabled=Fe();_globalRippleOptions=d(Q_,{optional:!0});_platform=d(we);_ngZone=d(G);_injector=d(B);_eventCleanups;_hosts=new Map;constructor(){let e=d(bt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>vF.map(i=>e.listen(this._document,i,this._onInteraction,_F)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Y_,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(K_))&&e.setAttribute(K_,i.className||""),i.centered&&e.setAttribute(Wx,""),i.disabled&&e.setAttribute(Sf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Sf,""):e.removeAttribute(Sf)}_onInteraction=e=>{let i=Tt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Y_}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(K_)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Gl.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??Gl.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Sf),rippleConfig:{centered:e.hasAttribute(Wx),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Wl(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Y_)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ii=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return t})();var bF=["mat-icon-button",""],yF=["*"],wF=new y("MAT_BUTTON_CONFIG");function Qx(t){return t==null?void 0:zt(t)}var Z_=(()=>{class t{_elementRef=d(F);_ngZone=d(G);_animationsDisabled=Fe();_config=d(wF,{optional:!0});_focusMonitor=d(Ot);_cleanupClick;_renderer=d(Te);_rippleLoader=d(qx);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(st).load(ii);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(J("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),at(r.color?"mat-"+r.color:""),P("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",ee],disabled:[2,"disabled","disabled",ee],ariaDisabled:[2,"aria-disabled","ariaDisabled",ee],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ee],tabIndex:[2,"tabIndex","tabIndex",Qx],_tabindex:[2,"tabindex","_tabindex",Qx]}})}return t})(),go=(()=>{class t extends Z_{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[ve],attrs:bF,ngContentSelectors:yF,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(me(),Qt(0,"span",0),z(1),Qt(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2,changeDetection:0})}return t})();var ri=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var CF=["matButton",""],DF=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],xF=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var Yx=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),oi=(()=>{class t extends Z_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=EF(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Yx.get(this._appearance):null,o=Yx.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[ve],attrs:CF,ngContentSelectors:xF,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(me(DF),Qt(0,"span",0),z(1),nt(2,"span",1),z(3,1),gt(),z(4,2),Qt(5,"span",2)(6,"span",3)),i&2&&P("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return t})();function EF(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var _o=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ri,ae]})}return t})();function IF(t,n){if(t&1){let e=it();h(0,"div",1)(1,"button",2),W("click",function(){De(e);let r=D();return xe(r.action())}),v(2),p()()}if(t&2){let e=D();m(2),E(" ",e.data.action," ")}}var SF=["label"];function MF(t,n){}var TF=Math.pow(2,31)-1,ql=class{_overlayRef;instance;containerInstance;_afterDismissed=new x;_afterOpened=new x;_onAction=new x;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,TF))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Kx=new y("MatSnackBarData"),Aa=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},kF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),RF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),AF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),OF=(()=>{class t{snackBarRef=d(ql);data=d(Kx);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(h(0,"div",0),v(1),p(),k(2,IF,3,1,"div",1)),i&2&&(m(),E(" ",r.data.message,`
`),m(),R(r.hasAction?2:-1))},dependencies:[oi,kF,RF,AF],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),X_="_mat-snack-bar-enter",J_="_mat-snack-bar-exit",NF=(()=>{class t extends sr{_ngZone=d(G);_elementRef=d(F);_changeDetectorRef=d(ye);_platform=d(we);_animationsDisabled=Fe();snackBarConfig=d(Aa);_document=d(K);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(B);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new x;_onExit=new x;_onEnter=new x;_animationState="void";_live;_label;_role;_liveElementId=d(He).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===J_?this._completeExit():e===X_&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?$e(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(X_)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(X_)},200)))}exit(){return this._destroyed?Z(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?$e(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(J_)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(J_),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Ne(lr,7)(SF,7),i&2){let o;q(o=Q())&&(r._portalOutlet=o.first),q(o=Q())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&W("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&P("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[ve],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(h(0,"div",1)(1,"div",2,0)(3,"div",3),ze(4,MF,0,0,"ng-template",4),p(),ie(5,"div"),p()()),i&2&&(m(5),J("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[lr],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2})}return t})(),FF=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Aa}),Zx=(()=>{class t{_live=d(Al);_injector=d(B);_breakpointObserver=d(Fn);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(FF);_animationsDisabled=Fe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=OF;snackBarContainerComponent=NF;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=b(b({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=B.create({parent:r||this._injector,providers:[{provide:Aa,useValue:i}]}),a=new Ln(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=b(b(b({},new Aa),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new ql(a,o);if(e instanceof ht){let l=new Bn(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new Ln(e,void 0,l),u=a.attachComponentPortal(c);s.instance=u.instance}return this._breakpointObserver.observe(ar.HandsetPortrait).pipe(fe(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new jn;i.direction=e.direction;let r=dr(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Hn(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return B.create({parent:r||this._injector,providers:[{provide:ql,useValue:i},{provide:Kx,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ev(t){t||(t=d(mt));let n=new re(e=>{if(t.destroyed){e.next();return}return t.onDestroy(e.next.bind(e))});return e=>e.pipe(fe(n))}var iv=class{translations;constructor(n){this.translations=n}getTranslation(n){return Z(this.translations.get(n)||{})}},eE=new y("");function tv(t,n){return t&&(Object.prototype.hasOwnProperty.call(t,n)?t[n]:n.split(".").reduce((e,i)=>e?.[i],t))}function PF(t,n,e){t=b({},t);let i=n.split("."),r=i.length-1;return i.reduce((o,a,s)=>(s===r?o[a]=e:o[a]=Array.isArray(o[a])?o[a].slice():b({},o[a]),o&&o[a]),t),t}function tE(t){return t?Array.isArray(t)?t.length:kf(t)?Object.keys(t).length:t?t.length:0:0}function LF(t){return tE(t)===0}function BF(t){return typeof t=="function"}function Na(t){return typeof t=="string"}function kf(t){return!!t&&typeof t=="object"&&!Array.isArray(t)}function nE(t){return t.replace(/(?:^\w|[A-Z]|\b\w)/g,(n,e)=>e==0?n.toLowerCase():n.toUpperCase()).replace(/\s+|_|-|\//g,"")}function iE(){return typeof window<"u"}function rv(t){return t==null}function Xx(t){return rv(t)===!1}function rE(t){return t&&typeof t.scope=="string"}function jF(t){return t&&kf(t.loader)}function Jx(t){let n={};function e(i,r){if(i===null)n[r]=null;else if(kf(i))for(let[o,a]of Object.entries(i))e(a,r?`${r}.${o}`:o);else n[r]=i}return e(t,""),n}function VF(t){let n={};for(let[e,i]of Object.entries(t)){let r=e.split("."),o=n;r.forEach((a,s)=>{s===r.length-1?o[a]=i:(o[a]??={},o=o[a])})}return n}var Fa=new y("",{providedIn:"root",factory:()=>Oa}),Oa={defaultLang:"en",reRenderOnLangChange:!1,prodMode:!1,failedRetries:2,fallbackLang:[],availableLangs:[],missingHandler:{logMissingKey:!0,useFallbackTranslation:!1,allowEmpty:!1},flatten:{aot:!1},interpolation:["{{","}}"],scopes:{keepCasing:!1}};function HF(t={}){return X(b(b({},Oa),t),{missingHandler:b(b({},Oa.missingHandler),t.missingHandler),flatten:b(b({},Oa.flatten),t.flatten),scopes:b(b({},Oa.scopes),t.scopes)})}var oE=new y(""),zF=(()=>{class t{config=d(Fa,{optional:!0})??Oa;get interpolationMatcher(){return UF(this.config)}transpile({value:e,params:i={},translation:r,key:o}){if(Na(e)){let a,s=e;for(;(a=this.interpolationMatcher.exec(s))!==null;){let[l,c]=a;s=s.replace(l,()=>{let u=c.trim(),f=tv(i,u);return Xx(f)?f:Xx(r[u])?this.transpile({params:i,translation:r,key:o,value:r[u]}):""})}return s}else i&&(kf(e)?e=this.handleObject({value:e,params:i,translation:r,key:o}):Array.isArray(e)&&(e=this.handleArray({value:e,params:i,translation:r,key:o})));return e}handleObject({value:e,params:i={},translation:r,key:o}){let a=e;return Object.keys(i).forEach(s=>{let l=this.transpile({value:tv(a,s),params:tv(i,s),translation:r,key:o});a=PF(a,s,l)}),a}handleArray(r){var o=r,{value:e}=o,i=wm(o,["value"]);return e.map(a=>this.transpile(b({value:a},i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function UF(t){let[n,e]=t.interpolation;return new RegExp(`${n}([^${n}${e}]*?)${e}`,"g")}var aE=new y(""),$F=(()=>{class t{handle(e,i){if(i.missingHandler.logMissingKey&&!i.prodMode){let r=`Missing translation for '${e}'`;console.warn(`%c ${r}`,"font-size: 12px; color: red")}return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),sE=new y(""),GF=(()=>{class t{preSaveTranslation(e){return e}preSaveTranslationKey(e,i){return i}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),lE=new y(""),WF=(()=>{class t{userConfig;constructor(e){this.userConfig=e}getNextLangs(){let e=this.userConfig.fallbackLang;if(!e)throw new Error("When using the default fallback, a fallback language must be provided in the config!");return Array.isArray(e)?e:[e]}static \u0275fac=function(i){return new(i||t)(H(Fa))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function Ql(t){if(!t)return"";let n=t.split("/");return n.pop(),n.join("/")}function fr(t){return t?t.split("/").pop():""}function ov(t,n,e="|"){if(Na(t)){let i=t.split(e),r=i.pop();return r===n?[!0,i.toString()]:[!1,r]}return[!1,""]}function cE(t,n){let[e]=ov(n,"static");return e?!1:!!t.config.reRenderOnLangChange}function dE(t){return t?n=>n:Ce(1)}function qF(t,n){return Object.keys(t).reduce((e,i)=>(e[`${n}/${i}`]=t[i],e),{})}function sv(t,n){return jF(t)?qF(t.loader,n):void 0}function nv(t){return{scope:Ql(t)||null,langName:fr(t)}}function uE(t){let{path:n,inlineLoader:e,mainLoader:i,data:r}=t;if(e){let o=e[n];if(BF(o)===!1)throw`You're using an inline loader but didn't provide a loader for ${n}`;return e[n]().then(a=>a.default?a.default:a)}return i.getTranslation(n,r)}function QF({mainLoader:t,path:n,data:e,fallbackPath:i,inlineLoader:r}){return(i?[n,i]:[n]).map(a=>{let s=uE({path:a,mainLoader:t,inlineLoader:r,data:e});return je(s).pipe(U(l=>({translation:l,lang:a})))})}var YF;var Pa=(()=>{class t{loader;parser;missingHandler;interceptor;fallbackStrategy;langChanges$;translations=new Map;cache=new Map;firstFallbackLang;defaultLang="";availableLangs=[];isResolvedMissingOnce=!1;lang;failedLangs=new Set;events=new x;events$=this.events.asObservable();config;destroyRef=d(mt);constructor(e,i,r,o,a,s){this.loader=e,this.parser=i,this.missingHandler=r,this.interceptor=o,this.fallbackStrategy=s,this.loader||(this.loader=new iv(this.translations)),YF=this,this.config=JSON.parse(JSON.stringify(a)),this.setAvailableLangs(this.config.availableLangs||[]),this.setFallbackLangForMissingTranslation(this.config),this.setDefaultLang(this.config.defaultLang),this.lang=new Xe(this.getDefaultLang()),this.langChanges$=this.lang.asObservable(),this.events$.subscribe(l=>{l.type==="translationLoadSuccess"&&l.wasFailure&&this.setActiveLang(l.payload.langName)}),this.destroyRef.onDestroy(()=>{this.lang.complete(),this.events.complete(),this.cache.clear()})}getDefaultLang(){return this.defaultLang}setDefaultLang(e){this.defaultLang=e}getActiveLang(){return this.lang.getValue()}setActiveLang(e){return this.parser.onLangChanged?.(e),this.lang.next(e),this.events.next({type:"langChanged",payload:nv(e)}),this}setAvailableLangs(e){this.availableLangs=e}getAvailableLangs(){return this.availableLangs}load(e,i={}){let r=this.cache.get(e);if(r)return r;let o,a=this._isLangScoped(e),s;a&&(s=Ql(e));let l={path:e,mainLoader:this.loader,inlineLoader:i.inlineLoader,data:a?{scope:s}:void 0};if(this.useFallbackTranslation(e)){let u=a?`${s}/${this.firstFallbackLang}`:this.firstFallbackLang,f=QF(X(b({},l),{fallbackPath:u}));o=ci(f)}else{let u=uE(l);o=je(u)}let c=o.pipe(jm(this.config.failedRetries),et(u=>{if(Array.isArray(u)){u.forEach(f=>{this.handleSuccess(f.lang,f.translation),f.lang!==e&&this.cache.set(f.lang,Z({}))});return}this.handleSuccess(e,u)}),$n(u=>(this.config.prodMode||console.error(`Error while trying to load "${e}"`,u),this.handleFailure(e,i))),Rr(1),ev(this.destroyRef));return this.cache.set(e,c),c}translate(e,i={},r=this.getActiveLang()){if(!e)return e;let{scope:o,resolveLang:a}=this.resolveLangAndScope(r);if(Array.isArray(e))return e.map(c=>this.translate(o?`${o}.${c}`:c,i,a));e=o?`${o}.${e}`:e;let s=this.getTranslation(a),l=s[e];return l?this.parser.transpile({value:l,params:i,translation:s,key:e}):this._handleMissingKey(e,l,i)}selectTranslate(e,i,r,o=!1){let a,s=(c,u)=>this.load(c,u).pipe(U(()=>o?this.translateObject(e,i,c):this.translate(e,i,c)));if(rv(r))return this.langChanges$.pipe(Ie(c=>s(c)));if(r=Array.isArray(r)?r[0]:r,rE(r)){let c=r;r=c.scope,a=sv(c,c.scope)}if(r=r,this.isLang(r)||this.isScopeWithLang(r))return s(r);let l=r;return this.langChanges$.pipe(Ie(c=>s(`${l}/${c}`,{inlineLoader:a})))}isScopeWithLang(e){return this.isLang(fr(e))}translateObject(e,i={},r=this.getActiveLang()){if(Na(e)||Array.isArray(e)){let{resolveLang:a,scope:s}=this.resolveLangAndScope(r);if(Array.isArray(e))return e.map(u=>this.translateObject(s?`${s}.${u}`:u,i,a));let l=this.getTranslation(a);e=s?`${s}.${e}`:e;let c=VF(this.getObjectByKey(l,e));return LF(c)?this.translate(e,i,r):this.parser.transpile({value:c,params:i,translation:l,key:e})}let o=[];for(let[a,s]of this.getEntries(e))o.push(this.translateObject(a,s,r));return o}selectTranslateObject(e,i,r){if(Na(e)||Array.isArray(e))return this.selectTranslate(e,i,r,!0);let[[o,a],...s]=this.getEntries(e);return this.selectTranslateObject(o,a,r).pipe(U(l=>{let c=[l];for(let[u,f]of s)c.push(this.translateObject(u,f,r));return c}))}getTranslation(e){if(e){if(this.isLang(e))return this.translations.get(e)||{};{let{scope:i,resolveLang:r}=this.resolveLangAndScope(e),o=this.translations.get(r)||{};return this.getObjectByKey(o,i)}}return this.translations}selectTranslation(e){let i=this.langChanges$;if(e){let r=fr(e)!==e;this.isLang(e)||r?i=Z(e):i=this.langChanges$.pipe(U(o=>`${e}/${o}`))}return i.pipe(Ie(r=>this.load(r).pipe(U(()=>this.getTranslation(r)))))}setTranslation(e,i=this.getActiveLang(),r={}){let a=b(b({},{merge:!0,emitChange:!0}),r),s=Ql(i),l=e;if(s){let _=this.getMappedScope(s);l=Jx({[_]:e})}let c=s?fr(i):i,u=b(b({},a.merge&&this.getTranslation(c)),l),f=this.config.flatten.aot?u:Jx(u),g=this.interceptor.preSaveTranslation(f,c);this.translations.set(c,g),a.emitChange&&this.setActiveLang(this.getActiveLang())}setTranslationKey(e,i,r={}){let o=r.lang||this.getActiveLang(),a=this.interceptor.preSaveTranslationKey(e,i,o),s={[e]:a};this.setTranslation(s,o,X(b({},r),{merge:!0}))}setFallbackLangForMissingTranslation({fallbackLang:e}){let i=Array.isArray(e)?e[0]:e;e&&this.useFallbackTranslation(i)&&(this.firstFallbackLang=i)}_handleMissingKey(e,i,r){if(this.config.missingHandler.allowEmpty&&i==="")return"";if(!this.isResolvedMissingOnce&&this.useFallbackTranslation()){this.isResolvedMissingOnce=!0;let o=this.translate(e,r,this.firstFallbackLang);return this.isResolvedMissingOnce=!1,o}return this.missingHandler.handle(e,this.getMissingHandlerData(),r)}_isLangScoped(e){return this.getAvailableLangsIds().indexOf(e)===-1}isLang(e){return this.getAvailableLangsIds().indexOf(e)!==-1}_loadDependencies(e,i){let r=fr(e);return this._isLangScoped(e)&&!this.isLoadedTranslation(r)?Tr([this.load(r),this.load(e,{inlineLoader:i})]):this.load(e,{inlineLoader:i})}_completeScopeWithLang(e){return this._isLangScoped(e)&&!this.isLang(fr(e))?`${e}/${this.getActiveLang()}`:e}_setScopeAlias(e,i){this.config.scopeMapping||(this.config.scopeMapping={}),this.config.scopeMapping[e]=i}isLoadedTranslation(e){return tE(this.getTranslation(e))}getAvailableLangsIds(){let e=this.getAvailableLangs()[0];return Na(e)?this.getAvailableLangs():this.getAvailableLangs().map(i=>i.id)}getMissingHandlerData(){return X(b({},this.config),{activeLang:this.getActiveLang(),availableLangs:this.availableLangs,defaultLang:this.defaultLang})}useFallbackTranslation(e){return this.config.missingHandler.useFallbackTranslation&&e!==this.firstFallbackLang}handleSuccess(e,i){this.setTranslation(i,e,{emitChange:!1}),this.events.next({wasFailure:!!this.failedLangs.size,type:"translationLoadSuccess",payload:nv(e)}),this.failedLangs.forEach(r=>this.cache.delete(r)),this.failedLangs.clear()}handleFailure(e,i){rv(i.failedCounter)&&(i.failedCounter=0,i.fallbackLangs||(i.fallbackLangs=this.fallbackStrategy.getNextLangs(e)));let r=e.split("/"),a=i.fallbackLangs[i.failedCounter];if(this.failedLangs.add(e),this.cache.has(a))return this.handleSuccess(a,this.getTranslation(a)),Je;let s=a===r[r.length-1];if(!a||s){let c="Unable to load translation and all the fallback languages";throw r.length>1&&(c+=", did you misspelled the scope name?"),new Error(c)}let l=a;return r.length>1&&(r[r.length-1]=a,l=r.join("/")),i.failedCounter++,this.events.next({type:"translationLoadFailure",payload:nv(e)}),this.load(l,i)}getMappedScope(e){let{scopeMapping:i={},scopes:r={keepCasing:!1}}=this.config;return i[e]||(r.keepCasing?e:nE(e))}resolveLangAndScope(e){let i=e,r;if(this._isLangScoped(e)){let o=fr(e),a=this.isLang(o);i=a?o:this.getActiveLang(),r=this.getMappedScope(a?Ql(e):e)}return{scope:r,resolveLang:i}}getObjectByKey(e,i){let r={},o=`${i}.`;for(let a in e)a.startsWith(o)&&(r[a.replace(o,"")]=e[a]);return r}getEntries(e){return e instanceof Map?e.entries():Object.entries(e)}static \u0275fac=function(i){return new(i||t)(H(eE,8),H(oE),H(aE),H(sE),H(Fa),H(lE))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),KF=(()=>{class t{html;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["ng-component"]],inputs:{html:"html"},decls:1,vars:1,consts:[[1,"transloco-loader-template",3,"innerHTML"]],template:function(i,r){i&1&&Qt(0,"div",0),i&2&&Mt("innerHTML",r.html,Yp)},encapsulation:2})}return t})(),av=class{view;vcr;constructor(n,e){this.view=n,this.vcr=e}attachView(){if(this.view instanceof ht)this.vcr.createEmbeddedView(this.view);else if(Na(this.view)){let n=this.vcr.createComponent(KF);n.instance.html=this.view,n.hostView.detectChanges()}else this.vcr.createComponent(this.view)}detachView(){this.vcr.clear()}},fE=new y(""),ZF=new y(""),mE=new y(""),Mf=class{initialized=!1;resolve({inline:n,provider:e,active:i}){let r=i;if(this.initialized)return r=i,r;if(e){let[,o]=ov(e,"static");r=o}if(n){let[,o]=ov(n,"static");r=o}return this.initialized=!0,r}resolveLangBasedOnScope(n){return Ql(n)?fr(n):n}resolveLangPath(n,e){return e?`${e}/${n}`:n}},Tf=class{service;constructor(n){this.service=n}resolve(n){let{inline:e,provider:i}=n;if(e)return e;if(i){if(rE(i)){let{scope:r,alias:o=this.service.config.scopes.keepCasing?r:nE(r)}=i;return this.service._setScopeAlias(r,o),r}return i}}},mr=(()=>{class t{destroyRef=d(mt);service=d(Pa);tpl=d(ht,{optional:!0});providerLang=d(fE,{optional:!0});providerScope=d(mE,{optional:!0});providedLoadingTpl=d(ZF,{optional:!0});cdr=d(ye);host=d(F);vcr=d(pt);renderer=d(Te);view;memo=new Map;key;params={};inlineScope;inlineRead;prefix;inlineLang;inlineTpl;currentLang;loaderTplHandler;initialized=!1;path;langResolver=new Mf;scopeResolver=new Tf(this.service);strategy=this.tpl===null?"attribute":"structural";static ngTemplateContextGuard(e,i){return!0}ngOnInit(){let e=cE(this.service,this.providerLang||this.inlineLang);if(this.service.langChanges$.pipe(Ie(i=>{let r=this.langResolver.resolve({inline:this.inlineLang,provider:this.providerLang,active:i});return Array.isArray(this.providerScope)?ci(this.providerScope.map(o=>this.resolveScope(r,o))):this.resolveScope(r,this.providerScope)}),dE(e),ev(this.destroyRef)).subscribe(()=>{this.currentLang=this.langResolver.resolveLangBasedOnScope(this.path),this.strategy==="attribute"?this.attributeStrategy():this.structuralStrategy(this.currentLang,this.prefix||this.inlineRead),this.cdr.markForCheck(),this.initialized=!0}),!this.initialized){let i=this.resolveLoadingContent();i&&(this.loaderTplHandler=new av(i,this.vcr),this.loaderTplHandler.attachView())}}ngOnChanges(e){this.strategy==="attribute"&&Object.keys(e).some(r=>!e[r].firstChange)&&this.attributeStrategy()}attributeStrategy(){this.detachLoader(),this.renderer.setProperty(this.host.nativeElement,"innerText",this.service.translate(this.key,this.params,this.currentLang))}structuralStrategy(e,i){this.memo.clear();let r=this.getTranslateFn(e,i);this.view?(this.view.context.$implicit=r,this.view.context.currentLang=this.currentLang):(this.detachLoader(),this.view=this.vcr.createEmbeddedView(this.tpl,{$implicit:r,currentLang:this.currentLang}))}getTranslateFn(e,i){return(r,o)=>{let a=i?`${i}.${r}`:r,s=o?`${a}${JSON.stringify(o)}`:a;return this.memo.has(s)||this.memo.set(s,this.service.translate(a,o,e)),this.memo.get(s)}}resolveLoadingContent(){return this.inlineTpl||this.providedLoadingTpl}ngOnDestroy(){this.memo.clear()}detachLoader(){this.loaderTplHandler?.detachView()}resolveScope(e,i){let r=this.scopeResolver.resolve({inline:this.inlineScope,provider:i});this.path=this.langResolver.resolveLangPath(e,r);let o=sv(i,r);return this.service._loadDependencies(this.path,o)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","transloco",""]],inputs:{key:[0,"transloco","key"],params:[0,"translocoParams","params"],inlineScope:[0,"translocoScope","inlineScope"],inlineRead:[0,"translocoRead","inlineRead"],prefix:[0,"translocoPrefix","prefix"],inlineLang:[0,"translocoLang","inlineLang"],inlineTpl:[0,"translocoLoadingTpl","inlineTpl"]},features:[Ue]})}return t})(),La=(()=>{class t{service;providerScope;providerLang;cdr;subscription=null;lastValue="";lastKey;path;langResolver=new Mf;scopeResolver;constructor(e,i,r,o){this.service=e,this.providerScope=i,this.providerLang=r,this.cdr=o,this.scopeResolver=new Tf(this.service)}transform(e,i,r){if(!e)return e;let o=i?`${e}${JSON.stringify(i)}`:e;if(o===this.lastKey)return this.lastValue;this.lastKey=o,this.subscription?.unsubscribe();let a=cE(this.service,this.providerLang||r);return this.subscription=this.service.langChanges$.pipe(Ie(s=>{let l=this.langResolver.resolve({inline:r,provider:this.providerLang,active:s});return Array.isArray(this.providerScope)?ci(this.providerScope.map(c=>this.resolveScope(l,c))):this.resolveScope(l,this.providerScope)}),dE(a)).subscribe(()=>this.updateValue(e,i)),this.lastValue}ngOnDestroy(){this.subscription?.unsubscribe(),this.subscription=null}updateValue(e,i){let r=this.langResolver.resolveLangBasedOnScope(this.path);this.lastValue=this.service.translate(e,i,r),this.cdr.markForCheck()}resolveScope(e,i){let r=this.scopeResolver.resolve({inline:void 0,provider:i});this.path=this.langResolver.resolveLangPath(e,r);let o=sv(i,r);return this.service._loadDependencies(this.path,o)}static \u0275fac=function(i){return new(i||t)(_e(Pa,16),_e(mE,24),_e(fE,24),_e(ye,16))};static \u0275pipe=cu({name:"transloco",type:t,pure:!1})}return t})();var hE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();function pE(t){let n=[eP(zF),nP($F),iP(GF),tP(WF)];return t.config&&n.push(XF(t.config)),t.loader&&n.push(JF(t.loader)),n}function XF(t){return xt([{provide:Fa,useValue:HF(t)}])}function JF(t){return xt([{provide:eE,useClass:t}])}function eP(t){return xt([{provide:oE,useClass:t,deps:[Fa]}])}function tP(t){return xt([{provide:lE,useClass:t,deps:[Fa]}])}function nP(t){return xt([{provide:aE,useClass:t}])}function iP(t){return xt([{provide:sE,useClass:t}])}function gE(){let t=rP();if(!(!t||!iE()))return t.indexOf("-")!==-1&&(t=t.split("-")[0]),t.indexOf("_")!==-1&&(t=t.split("_")[0]),t}function rP(){if(!iE())return"";let t=window.navigator;return t.languages?.[0]??t.language}var Rf=class t{title="gwt-randomizers";swUpdate=d(lf);snackbar=d(Zx);translocoService=d(Pa);ngOnInit(){this.swUpdate.unrecoverable.subscribe(n=>{this.snackbar.open(`An error occurred that we cannot recover from:
`+n.reason+`

Please reload the page.`,"Reload").onAction().subscribe(()=>{window.location.reload()}),console.debug(`An error occurred that we cannot recover from:
`+n.reason+`

Please reload the page.`)}),this.swUpdate.versionUpdates.pipe(ue(n=>n.type==="VERSION_DETECTED")).subscribe(()=>{this.snackbar.open(this.translocoService.translate("messages.update-available"),"Reload").onAction().subscribe(()=>{window.location.reload()})})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&ie(0,"router-outlet")},dependencies:[xl,hE],encapsulation:2})};var Af=class t{http=d(ra);getTranslation(n){return this.http.get(`i18n/${n}.json`)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Of=class t{playerCount=new $;neutralBuildings=[{title:"A",sides:[{title:"front"}]},{title:"B",sides:[{title:"front"}]},{title:"C",sides:[{title:"front"}]},{title:"D",sides:[{title:"front"}]},{title:"E",sides:[{title:"front"}]},{title:"F",sides:[{title:"front"}]},{title:"G",sides:[{title:"front"}]},{title:"H",sides:[{title:"front"}]}];playerBuildings=[{title:"1",sides:[{title:"a"},{title:"b"}]},{title:"2",sides:[{title:"a"},{title:"b"}]},{title:"3",sides:[{title:"a"},{title:"b"}]},{title:"4",sides:[{title:"a"},{title:"b"}]},{title:"5",sides:[{title:"a"},{title:"b"}]},{title:"6",sides:[{title:"a"},{title:"b"}]},{title:"7",sides:[{title:"a"},{title:"b"}]},{title:"8",sides:[{title:"a"},{title:"b"}]},{title:"9",sides:[{title:"a"},{title:"b"}]},{title:"10",sides:[{title:"a"},{title:"b"}]}];stationMasters=[{title:"1",sides:[{title:"front",image:"img/station-master-01.png"}]},{title:"2",sides:[{title:"front",image:"img/station-master-02.png"}]},{title:"3",sides:[{title:"front",image:"img/station-master-03.png"}]},{title:"4",sides:[{title:"front",image:"img/station-master-04.png"}]},{title:"5",sides:[{title:"front",image:"img/station-master-05.png"}]},{title:"6",sides:[{title:"front",image:"img/station-master-06.png"}]},{title:"7",sides:[{title:"front",image:"img/station-master-07.png"}]},{title:"8",sides:[{title:"front",image:"img/station-master-08.png"}]}];cities=[{title:"Le Havre",sides:[{title:"a"},{title:"b"}]},{title:"Rotterdam",sides:[{title:"a"},{title:"b"}]},{title:"Liverpool",sides:[{title:"a"},{title:"b"}]}];getRandomNeutralBuildingOrder(){return this.shuffleArray(this.neutralBuildings)}getRandomStationMasters(){let n=[],e=this.shuffleArray(this.stationMasters);for(let i=0;i<5;i+=1)n.push(e.pop());return n}getRandomPlayerBuildings(){let n=JSON.parse(JSON.stringify(this.playerBuildings));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}getRandomCities(){let n=JSON.parse(JSON.stringify(this.cities));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}shuffleArray(n){let e=n.slice();for(let i,r,o=e.length;o;i=Math.floor(Math.random()*o),r=e[--o],e[o]=e[i],e[i]=r);return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var _E=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[Vl]})}return t})();var Yl=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new x;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var lv=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[_E,ae]})}return t})();var cv=class{_box;_destroyed=new x;_resizeSubject=new x;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new re(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ue(e=>e.some(i=>i.target===n)),Rr({bufferSize:1,refCount:!0}),fe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},bE=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(G);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new cv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var oP=["notch"],aP=["matFormFieldNotchedOutline",""],sP=["*"],yE=["iconPrefixContainer"],wE=["textPrefixContainer"],CE=["iconSuffixContainer"],DE=["textSuffixContainer"],lP=["textField"],cP=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],dP=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function uP(t,n){t&1&&ie(0,"span",21)}function fP(t,n){if(t&1&&(h(0,"label",20),z(1,1),k(2,uP,1,0,"span",21),p()),t&2){let e=D(2);j("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),J("for",e._control.disableAutomaticLabeling?null:e._control.id),m(2),R(!e.hideRequiredMarker&&e._control.required?2:-1)}}function mP(t,n){if(t&1&&k(0,fP,3,5,"label",20),t&2){let e=D();R(e._hasFloatingLabel()?0:-1)}}function hP(t,n){t&1&&ie(0,"div",7)}function pP(t,n){}function gP(t,n){if(t&1&&ze(0,pP,0,0,"ng-template",13),t&2){D(2);let e=Ct(1);j("ngTemplateOutlet",e)}}function _P(t,n){if(t&1&&(h(0,"div",9),k(1,gP,1,1,null,13),p()),t&2){let e=D();j("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),m(),R(e._forceDisplayInfixLabel()?-1:1)}}function vP(t,n){t&1&&(h(0,"div",10,2),z(2,2),p())}function bP(t,n){t&1&&(h(0,"div",11,3),z(2,3),p())}function yP(t,n){}function wP(t,n){if(t&1&&ze(0,yP,0,0,"ng-template",13),t&2){D();let e=Ct(1);j("ngTemplateOutlet",e)}}function CP(t,n){t&1&&(h(0,"div",14,4),z(2,4),p())}function DP(t,n){t&1&&(h(0,"div",15,5),z(2,5),p())}function xP(t,n){t&1&&ie(0,"div",16)}function EP(t,n){t&1&&(h(0,"div",18),z(1,6),p())}function IP(t,n){if(t&1&&(h(0,"mat-hint",22),v(1),p()),t&2){let e=D(2);j("id",e._hintLabelId),m(),V(e.hintLabel)}}function SP(t,n){if(t&1&&(h(0,"div",19),k(1,IP,2,2,"mat-hint",22),z(2,7),ie(3,"div",23),z(4,8),p()),t&2){let e=D();m(),R(e.hintLabel?1:-1)}}var hr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-label"]]})}return t})(),MP=new y("MatError");var Ff=(()=>{class t{align="start";id=d(He).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Mt("id",r.id),J("align",null),P("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),TP=new y("MatPrefix");var kP=new y("MatSuffix");var kE=new y("FloatingLabelParent"),xE=(()=>{class t{_elementRef=d(F);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(bE);_ngZone=d(G);_parent=d(kE);_resizeSubscription=new be;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return RP(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function RP(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var EE="mdc-line-ripple--active",Nf="mdc-line-ripple--deactivating",IE=(()=>{class t{_elementRef=d(F);_cleanupTransitionEnd;constructor(){let e=d(G),i=d(Te);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Nf),e.add(EE)}deactivate(){this._elementRef.nativeElement.classList.add(Nf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Nf);e.propertyName==="opacity"&&r&&i.remove(EE,Nf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),SE=(()=>{class t{_elementRef=d(F);_ngZone=d(G);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ne(oP,5),i&2){let o;q(o=Q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:aP,ngContentSelectors:sP,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(me(),Qt(0,"div",1),nt(1,"div",2,0),z(3),gt(),Qt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),dv=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t})}return t})();var uv=new y("MatFormField"),AP=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),ME="fill",OP="auto",TE="fixed",NP="translateY(-50%)",Ri=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(ye);_platform=d(we);_idGenerator=d(He);_ngZone=d(G);_defaults=d(AP,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=tl("iconPrefixContainer");_textPrefixContainerSignal=tl("textPrefixContainer");_iconSuffixContainerSignal=tl("iconSuffixContainer");_textSuffixContainerSignal=tl("textSuffixContainer");_prefixSuffixContainers=Ht(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=wC(hr);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=$t(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||OP}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||ME;this._appearanceSignal.set(i)}_appearanceSignal=le(ME);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||TE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||TE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new x;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Fe();constructor(){let e=this._defaults,i=d(Ke);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),qi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Ht(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Ge([void 0,void 0]),U(()=>[i.errorState,i.userAriaDescribedBy]),Gc(),ue(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(fe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Pt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){xC({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Ht(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${a+s}px`,_=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,C=`var(--mat-mdc-form-field-label-transform, ${NP} translateX(${_}))`,O=a+s+l+c;return[C,O]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(hu(o,r._labelChild,hr,5),Ye(o,dv,5)(o,TP,5)(o,kP,5)(o,MP,5)(o,Ff,5)),i&2){gu();let a;q(a=Q())&&(r._formFieldControl=a.first),q(a=Q())&&(r._prefixChildren=a),q(a=Q())&&(r._suffixChildren=a),q(a=Q())&&(r._errorChildren=a),q(a=Q())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(pu(r._iconPrefixContainerSignal,yE,5)(r._textPrefixContainerSignal,wE,5)(r._iconSuffixContainerSignal,CE,5)(r._textSuffixContainerSignal,DE,5),Ne(lP,5)(yE,5)(wE,5)(CE,5)(DE,5)(xE,5)(SE,5)(IE,5)),i&2){gu(4);let o;q(o=Q())&&(r._textField=o.first),q(o=Q())&&(r._iconPrefixContainer=o.first),q(o=Q())&&(r._textPrefixContainer=o.first),q(o=Q())&&(r._iconSuffixContainer=o.first),q(o=Q())&&(r._textSuffixContainer=o.first),q(o=Q())&&(r._floatingLabel=o.first),q(o=Q())&&(r._notchedOutline=o.first),q(o=Q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&P("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Se([{provide:uv,useExisting:t},{provide:kE,useExisting:t}])],ngContentSelectors:dP,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(me(cP),ze(0,mP,1,1,"ng-template",null,0,vu),h(2,"div",6,1),W("click",function(a){return r._control.onContainerClick(a)}),k(4,hP,1,0,"div",7),h(5,"div",8),k(6,_P,2,2,"div",9),k(7,vP,3,0,"div",10),k(8,bP,3,0,"div",11),h(9,"div",12),k(10,wP,1,1,null,13),z(11),p(),k(12,CP,3,0,"div",14),k(13,DP,3,0,"div",15),p(),k(14,xP,1,0,"div",16),p(),h(15,"div",17),k(16,EP,2,0,"div",18)(17,SP,5,1,"div",19),p()),i&2){let o;m(2),P("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),m(2),R(!r._hasOutline()&&!r._control.disabled?4:-1),m(2),R(r._hasOutline()?6:-1),m(),R(r._hasIconPrefix?7:-1),m(),R(r._hasTextPrefix?8:-1),m(2),R(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),m(2),R(r._hasTextSuffix?12:-1),m(),R(r._hasIconSuffix?13:-1),m(),R(r._hasOutline()?-1:14),m(),P("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();m(),R((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[xE,SE,Vg,IE,Ff],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var BE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(_e(Te),_e(F))};static \u0275dir=N({type:t})}return t})(),FP=(()=>{class t extends BE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275dir=N({type:t,features:[ve]})}return t})(),Qf=new y("");var PP={provide:Qf,useExisting:Zt(()=>jE),multi:!0};function LP(){let t=mn()?mn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var BP=new y(""),jE=(()=>{class t extends BE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!LP())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(_e(Te),_e(F),_e(BP,8))};static \u0275dir=N({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&W("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[Se([PP]),ve]})}return t})();function hv(t){return t==null||pv(t)===0}function pv(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var tc=new y(""),gv=new y(""),jP=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Lf=class{static min(n){return VP(n)}static max(n){return HP(n)}static required(n){return zP(n)}static requiredTrue(n){return UP(n)}static email(n){return $P(n)}static minLength(n){return GP(n)}static maxLength(n){return WP(n)}static pattern(n){return qP(n)}static nullValidator(n){return VE()}static compose(n){return WE(n)}static composeAsync(n){return qE(n)}};function VP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function HP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function zP(t){return hv(t.value)?{required:!0}:null}function UP(t){return t.value===!0?null:{required:!0}}function $P(t){return hv(t.value)||jP.test(t.value)?null:{email:!0}}function GP(t){return n=>{let e=n.value?.length??pv(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function WP(t){return n=>{let e=n.value?.length??pv(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function qP(t){if(!t)return VE;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(hv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function VE(t){return null}function HE(t){return t!=null}function zE(t){return Zi(t)?je(t):t}function UE(t){let n={};return t.forEach(e=>{n=e!=null?b(b({},n),e):n}),Object.keys(n).length===0?null:n}function $E(t,n){return n.map(e=>e(t))}function QP(t){return!t.validate}function GE(t){return t.map(n=>QP(n)?n:e=>n.validate(e))}function WE(t){if(!t)return null;let n=t.filter(HE);return n.length==0?null:function(e){return UE($E(e,n))}}function _v(t){return t!=null?WE(GE(t)):null}function qE(t){if(!t)return null;let n=t.filter(HE);return n.length==0?null:function(e){let i=$E(e,n).map(zE);return ci(i).pipe(U(UE))}}function vv(t){return t!=null?qE(GE(t)):null}function RE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function QE(t){return t._rawValidators}function YE(t){return t._rawAsyncValidators}function fv(t){return t?Array.isArray(t)?t:[t]:[]}function Bf(t,n){return Array.isArray(t)?t.includes(n):t===n}function AE(t,n){let e=fv(n);return fv(t).forEach(r=>{Bf(e,r)||e.push(r)}),e}function OE(t,n){return fv(n).filter(e=>!Bf(t,e))}var jf=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=_v(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=vv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},vo=class extends jf{name;get formDirective(){return null}get path(){return null}},bo=class extends jf{_parent=null;name=null;valueAccessor=null},mv=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var KE=(()=>{class t extends mv{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(_e(bo,2))};static \u0275dir=N({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&P("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[ve]})}return t})();var Kl="VALID",Pf="INVALID",Ba="PENDING",Zl="DISABLED",pr=class{},Vf=class extends pr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Jl=class extends pr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},ec=class extends pr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},ja=class extends pr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Hf=class extends pr{source;constructor(n){super(),this.source=n}},zf=class extends pr{source;constructor(n){super(),this.source=n}};function ZE(t){return(Yf(t)?t.validators:t)||null}function YP(t){return Array.isArray(t)?_v(t):t||null}function XE(t,n){return(Yf(n)?n.asyncValidators:t)||null}function KP(t){return Array.isArray(t)?vv(t):t||null}function Yf(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function ZP(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new S(1e3,"");if(!i[e])throw new S(1001,"")}function XP(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new S(-1002,"")})}var Uf=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return ke(this.statusReactive)}set status(n){ke(()=>this.statusReactive.set(n))}_status=Ht(()=>this.statusReactive());statusReactive=le(void 0);get valid(){return this.status===Kl}get invalid(){return this.status===Pf}get pending(){return this.status===Ba}get disabled(){return this.status===Zl}get enabled(){return this.status!==Zl}errors;get pristine(){return ke(this.pristineReactive)}set pristine(n){ke(()=>this.pristineReactive.set(n))}_pristine=Ht(()=>this.pristineReactive());pristineReactive=le(!0);get dirty(){return!this.pristine}get touched(){return ke(this.touchedReactive)}set touched(n){ke(()=>this.touchedReactive.set(n))}_touched=Ht(()=>this.touchedReactive());touchedReactive=le(!1);get untouched(){return!this.touched}_events=new x;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(AE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(AE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(OE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(OE(n,this._rawAsyncValidators))}hasValidator(n){return Bf(this._rawValidators,n)}hasAsyncValidator(n){return Bf(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(X(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ec(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new ec(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(X(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Jl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Jl(!0,i))}markAsPending(n={}){this.status=Ba;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ja(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(X(b({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Zl,this.errors=null,this._forEachChild(r=>{r.disable(X(b({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Vf(this.value,i)),this._events.next(new ja(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(X(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Kl,this._forEachChild(i=>{i.enable(X(b({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(X(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Kl||this.status===Ba)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Vf(this.value,e)),this._events.next(new ja(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(X(b({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Zl:Kl}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Ba,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=zE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new ja(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new $,this.statusChanges=new $}_calculateStatus(){return this._allControlsDisabled()?Zl:this.errors?Pf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Ba)?Ba:this._anyControlsHaveStatus(Pf)?Pf:Kl}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Jl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new ec(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Yf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=YP(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=KP(this._rawAsyncValidators)}},$f=class extends Uf{constructor(n,e,i){super(ZE(e),XE(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){XP(this,!0,n),Object.keys(n).forEach(i=>{ZP(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,X(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new zf(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Kf=new y("",{factory:()=>bv}),bv="always";function JP(t,n){return[...n.path,t]}function Gf(t,n,e=bv){yv(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),tL(t,n),iL(t,n),nL(t,n),eL(t,n)}function NE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),qf(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Wf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function eL(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function yv(t,n){let e=QE(t);n.validator!==null?t.setValidators(RE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=YE(t);n.asyncValidator!==null?t.setAsyncValidators(RE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Wf(n._rawValidators,r),Wf(n._rawAsyncValidators,r)}function qf(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=QE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=YE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Wf(n._rawValidators,i),Wf(n._rawAsyncValidators,i),e}function tL(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&JE(t,n)})}function nL(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&JE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function JE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function iL(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function eI(t,n){t==null,yv(t,n)}function rL(t,n){return qf(t,n)}function oL(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function aL(t){return Object.getPrototypeOf(t.constructor)===FP}function tI(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function sL(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===jE?e=o:aL(o)?i=o:r=o}),r||i||e||null}function lL(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var cL={provide:vo,useExisting:Zt(()=>wv)},Xl=Promise.resolve(),wv=(()=>{class t extends vo{callSetDisabledState;get submitted(){return ke(this.submittedReactive)}_submitted=Ht(()=>this.submittedReactive());submittedReactive=le(!1);_directives=new Set;form;ngSubmit=new $;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new $f({},_v(e),vv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Xl.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Gf(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Xl.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Xl.then(()=>{let i=this._findContainer(e.path),r=new $f({});eI(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Xl.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Xl.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),tI(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Hf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(_e(tc,10),_e(gv,10),_e(Kf,8))};static \u0275dir=N({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&W("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Se([cL]),ve]})}return t})();function FE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function PE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var nI=class extends Uf{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(ZE(e),XE(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Yf(e)&&(e.nonNullable||e.initialValueIsDefault)&&(PE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new zf(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){FE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){FE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){PE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var dL=t=>t instanceof nI;var uL={provide:bo,useExisting:Zt(()=>Cv)},LE=Promise.resolve(),Cv=(()=>{class t extends bo{_changeDetectorRef;callSetDisabledState;control=new nI;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new $;constructor(e,i,r,o,a,s){super(),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=sL(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),oL(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Gf(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){LE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&ee(i);LE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?JP(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(_e(vo,9),_e(tc,10),_e(gv,10),_e(Qf,10),_e(ye,8),_e(Kf,8))};static \u0275dir=N({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Se([uL]),ve,Ue]})}return t})();var fL=(()=>{class t extends vo{callSetDisabledState;get submitted(){return ke(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Ht(()=>this._submittedReactive());_submittedReactive=le(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(qf(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Gf(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){NE(e.control||null,e,!1),lL(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,tI(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Hf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(NE(i||null,e),dL(r)&&(Gf(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);eI(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&rL(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){yv(this.form,this),this._oldForm&&qf(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(_e(tc,10),_e(gv,10),_e(Kf,8))};static \u0275dir=N({type:t,features:[ve,Ue]})}return t})();var mL={provide:vo,useExisting:Zt(()=>Dv)},Dv=(()=>{class t extends fL{form=null;ngSubmit=new $;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&W("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Se([mL]),ve]})}return t})();var hL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();var iI=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Kf,useValue:e.callSetDisabledState??bv}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[hL]})}return t})();var rI=(()=>{class t{_animationsDisabled=Fe();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&P("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();var gL=["text"],_L=[[["mat-icon"]],"*"],vL=["mat-icon","*"];function bL(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",1),t&2){let e=D();j("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function yL(t,n){if(t&1&&ie(0,"mat-pseudo-checkbox",3),t&2){let e=D();j("disabled",e.disabled)}}function wL(t,n){if(t&1&&(h(0,"span",4),v(1),p()),t&2){let e=D();m(),E("(",e.group.label,")")}}var Ev=new y("MAT_OPTION_PARENT_COMPONENT"),Iv=new y("MatOptgroup");var xv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},ai=(()=>{class t{_element=d(F);_changeDetectorRef=d(ye);_parent=d(Ev,{optional:!0});group=d(Iv,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(He).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=le(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new $;_text;_stateChanges=new x;constructor(){let e=d(st);e.load(ii),e.load(wa),this._signalDisableRipple=!!this._parent&&wi(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!lt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new xv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Ne(gL,7),i&2){let o;q(o=Q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&W("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(Mt("id",r.id),J("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),P("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",ee]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:vL,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(me(_L),k(0,bL,1,2,"mat-pseudo-checkbox",1),z(1),h(2,"span",2,0),z(4,1),p(),k(5,yL,1,1,"mat-pseudo-checkbox",3),k(6,wL,2,1,"span",4),ie(7,"div",5)),i&2&&(R(r.multiple?0:-1),m(5),R(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),m(),R(r.group&&r.group._inert?6:-1),m(),j("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[rI,ur],styles:[`.mat-mdc-option {
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
`],encapsulation:2,changeDetection:0})}return t})();function oI(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function aI(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var sI=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Zf=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var lI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[xa,Ri,ae]})}return t})();var Xf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var Sv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ri,Xf,ai,ae]})}return t})();var EL=["trigger"],IL=["panel"],SL=[[["mat-select-trigger"]],"*"],ML=["mat-select-trigger","*"];function TL(t,n){if(t&1&&(h(0,"span",4),v(1),p()),t&2){let e=D();m(),V(e.placeholder)}}function kL(t,n){t&1&&z(0)}function RL(t,n){if(t&1&&(h(0,"span",11),v(1),p()),t&2){let e=D(2);m(),V(e.triggerValue)}}function AL(t,n){if(t&1&&(h(0,"span",5),k(1,kL,1,0)(2,RL,2,1,"span",11),p()),t&2){let e=D();m(),R(e.customTrigger?1:2)}}function OL(t,n){if(t&1){let e=it();h(0,"div",12,1),W("keydown",function(r){De(e);let o=D();return xe(o._handleKeydown(r))}),z(2,1),p()}if(t&2){let e=D();at(e.panelClass),P("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),J("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var NL=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ki(t)}}),FL=new y("MAT_SELECT_CONFIG"),PL=new y("MatSelectTrigger"),Mv=class{source;value;constructor(n,e){this.source=n,this.value=e}},za=(()=>{class t{_viewportRuler=d(gn);_changeDetectorRef=d(ye);_elementRef=d(F);_dir=d(Ke,{optional:!0});_idGenerator=d(He);_renderer=d(Te);_parentFormField=d(uv,{optional:!0});ngControl=d(bo,{self:!0,optional:!0});_liveAnnouncer=d(Al);_defaultOptions=d(FL,{optional:!0});_animationsDisabled=Fe();_popoverLocation;_initialized=new x;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=oI(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=aI(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Mv(this,e)}_scrollStrategyFactory=d(NL);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new x;_errorStateTracker;stateChanges=new x;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=le(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Lf.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=wn(()=>{let e=this.options;return e?e.changes.pipe(Ge(e),Ie(()=>Pt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(Ie(()=>this.optionSelectionChanges))});openedChange=new $;_openedStream=this.openedChange.pipe(ue(e=>e),U(()=>{}));_closedStream=this.openedChange.pipe(ue(e=>!e),U(()=>{}));selectionChange=new $;valueChange=new $;constructor(){let e=d(sI),i=d(wv,{optional:!0}),r=d(Dv,{optional:!0}),o=d(new fn("tabindex"),{optional:!0}),a=d($l,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Zf(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Yl(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(fe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(fe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Ge(null),fe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ce(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&gf(this._trackedModal,"aria-owns",i),V_(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;gf(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!lt(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!lt(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!lt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ta?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Bl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Pt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(fe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Pt(...this.options.map(i=>i._stateChanges)).pipe(fe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Tt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,PL,5)(o,ai,5)(o,Iv,5),i&2){let a;q(a=Q())&&(r.customTrigger=a.first),q(a=Q())&&(r.options=a),q(a=Q())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&Ne(EL,5)(IL,5)(If,5),i&2){let o;q(o=Q())&&(r.trigger=o.first),q(o=Q())&&(r.panel=o.first),q(o=Q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&W("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(J("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),P("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",ee],disableRipple:[2,"disableRipple","disableRipple",ee],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zt(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",ee],placeholder:"placeholder",required:[2,"required","required",ee],multiple:[2,"multiple","multiple",ee],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",ee],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",zt],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",ee]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Se([{provide:dv,useExisting:t},{provide:Ev,useExisting:t}]),Ue],ngContentSelectors:ML,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(me(SL),h(0,"div",2,0),W("click",function(){return r.open()}),h(3,"div",3),k(4,TL,2,1,"span",4)(5,AL,3,1,"span",5),p(),h(6,"div",6)(7,"div",7),Wt(),h(8,"svg",8),ie(9,"path",9),p()()()(),ze(10,OL,3,16,"ng-template",10),W("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=Ct(1);m(3),J("id",r._valueId),m(),R(r.empty?4:5),m(6),j("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ta,If],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2,changeDetection:0})}return t})();var Jf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[_n,Sv,ae,pn,lI,Sv]})}return t})();var LL=["tooltip"],BL=20;var jL=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ki(t,{scrollThrottle:BL})}}),VL=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var dI="tooltip-panel",HL={passive:!0},zL=8,UL=8,$L=24,GL=200,uI=(()=>{class t{_elementRef=d(F);_ngZone=d(G);_platform=d(we);_ariaDescriber=d(Dx);_focusMonitor=d(Ot);_dir=d(Ke);_injector=d(B);_viewContainerRef=d(pt);_mediaMatcher=d(Da);_document=d(K);_renderer=d(Te);_animationsDisabled=Fe();_defaultOptions=d(VL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=WL;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=$t(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=$t(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Ut(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Ut(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new x;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=zL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(fe(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Ln(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(fe(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof F)return this._overlayRef;this._detach()}let i=this._injector.get(ni).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${dI}`,o=po(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(fe(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Hn(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(jL)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(fe(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(fe(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(fe(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(fe(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(b(b({},r.main),o.main)),this._addOffset(b(b({},r.fallback),o.fallback))])}_addOffset(e){let i=UL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),$e(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,a;if(i==="center"?this._dir&&this._dir.value==="rtl"?a=r==="end"?"left":"right":a=r==="start"?"left":"right":a=i==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let l=`${this._cssClassPrefix}-${dI}-`;s.removePanelClass(l+this._currentPosition),s.addPanelClass(l+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,HL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||$e({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!lt(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),WL=(()=>{class t{_changeDetectorRef=d(ye);_elementRef=d(F);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Fe();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new x;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>$L&&e.width>=GL}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(i);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Ne(LL,7),i&2){let o;q(o=Q())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&W("mouseleave",function(a){return r._handleMouseLeave(a)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(nt(0,"div",1,0),Xo("animationend",function(a){return r._handleAnimationEnd(a)}),nt(2,"div",2),v(3),gt()()),i&2&&(at(r.tooltipClass),P("mdc-tooltip--multiline",r._isMultiline),m(3),V(r.message))},styles:[`.mat-mdc-tooltip {
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
`],encapsulation:2,changeDetection:0})}return t})();var fI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[Ol,_n,ae,pn]})}return t})();function qL(t,n){if(t&1&&(h(0,"mat-option",17),v(1),p()),t&2){let e=n.$implicit;j("value",e),m(),E(" ",e," ")}}function QL(t,n){if(t&1){let e=it();h(0,"mat-form-field",14)(1,"mat-select",16,0),W("selectionChange",function(r){De(e);let o=D(2);return xe(o._changePageSize(r.value))}),It(3,qL,2,2,"mat-option",17,un),p(),h(5,"div",18),W("click",function(){De(e);let r=Ct(2);return xe(r.open())}),p()()}if(t&2){let e=D(2);j("appearance",e._formFieldAppearance)("color",e.color),m(),j("value",e.pageSize)("disabled",e.disabled),Ys("aria-labelledby",e._pageSizeLabelId),j("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),m(2),St(e._displayedPageSizeOptions)}}function YL(t,n){if(t&1&&(h(0,"div",15),v(1),p()),t&2){let e=D(2);m(),V(e.pageSize)}}function KL(t,n){if(t&1&&(h(0,"div",3)(1,"div",13),v(2),p(),k(3,QL,6,7,"mat-form-field",14),k(4,YL,2,1,"div",15),p()),t&2){let e=D();m(),J("id",e._pageSizeLabelId),m(),E(" ",e._intl.itemsPerPageLabel," "),m(),R(e._displayedPageSizeOptions.length>1?3:-1),m(),R(e._displayedPageSizeOptions.length<=1?4:-1)}}function ZL(t,n){if(t&1){let e=it();h(0,"button",19),W("click",function(){De(e);let r=D();return xe(r._buttonClicked(0,r._previousButtonsDisabled()))}),Wt(),h(1,"svg",8),ie(2,"path",20),p()()}if(t&2){let e=D();j("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),J("aria-label",e._intl.firstPageLabel)}}function XL(t,n){if(t&1){let e=it();h(0,"button",21),W("click",function(){De(e);let r=D();return xe(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),Wt(),h(1,"svg",8),ie(2,"path",22),p()()}if(t&2){let e=D();j("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),J("aria-label",e._intl.lastPageLabel)}}var JL=(()=>{class t{changes=new x;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,a=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${a} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),e2=50;var t2=new y("MAT_PAGINATOR_DEFAULT_OPTIONS"),n2=(()=>{class t{_intl=d(JL);_changeDetectorRef=d(ye);_formFieldAppearance;_pageSizeLabelId=d(He).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new yn(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>zt(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new $;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(t2,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:a,showFirstLastButtons:s}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),a!=null&&(this.hidePageSize=a),s!=null&&(this.showFirstLastButtons=s)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:e2),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",zt],length:[2,"length","length",zt],pageSize:[2,"pageSize","pageSize",zt],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",ee],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",ee],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",ee]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(h(0,"div",1)(1,"div",2),k(2,KL,5,4,"div",3),h(3,"div",4)(4,"div",5),v(5),p(),k(6,ZL,3,5,"button",6),h(7,"button",7),W("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),Wt(),h(8,"svg",8),ie(9,"path",9),p()(),Is(),h(10,"button",10),W("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),Wt(),h(11,"svg",8),ie(12,"path",11),p()(),k(13,XL,3,5,"button",12),p()()()),i&2&&(m(2),R(r.hidePageSize?-1:2),m(3),E(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),m(),R(r.showFirstLastButtons?6:-1),m(),j("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),J("aria-label",r._intl.previousPageLabel),m(3),j("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),J("aria-label",r._intl.nextPageLabel),m(3),R(r.showFirstLastButtons?13:-1))},dependencies:[Ri,za,ai,go,uI],styles:[`.mat-mdc-paginator {
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
`],encapsulation:2,changeDetection:0})}return t})(),mI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[_o,Jf,fI,n2]})}return t})();var i2=["*",[["mat-toolbar-row"]]],r2=["*","mat-toolbar-row"],o2=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),em=(()=>{class t{_elementRef=d(F);_platform=d(we);_document=d(K);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,o2,5),i&2){let a;q(a=Q())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(at(r.color?"mat-"+r.color:""),P("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:r2,decls:2,vars:0,template:function(i,r){i&1&&(me(i2),z(0),z(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2,changeDetection:0})}return t})();var hI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var gI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var nm=["*"],a2=["content"],s2=[[["mat-drawer"]],[["mat-drawer-content"]],"*"],l2=["mat-drawer","mat-drawer-content","*"];function c2(t,n){if(t&1){let e=it();h(0,"div",1),W("click",function(){De(e);let r=D();return xe(r._onBackdropClicked())}),p()}if(t&2){let e=D();P("mat-drawer-shown",e._isShowingBackdrop())}}function d2(t,n){t&1&&(h(0,"mat-drawer-content"),z(1,2),p())}var u2=[[["mat-sidenav"]],[["mat-sidenav-content"]],"*"],f2=["mat-sidenav","mat-sidenav-content","*"];function m2(t,n){if(t&1){let e=it();h(0,"div",1),W("click",function(){De(e);let r=D();return xe(r._onBackdropClicked())}),p()}if(t&2){let e=D();P("mat-drawer-shown",e._isShowingBackdrop())}}function h2(t,n){t&1&&(h(0,"mat-sidenav-content"),z(1,2),p())}var p2=`.mat-drawer-container {
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
`;var g2=new y("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),Rv=new y("MAT_DRAWER_CONTAINER"),tm=(()=>{class t extends Ti{_platform=d(we);_changeDetectorRef=d(ye);_container=d(kv);constructor(){let e=d(F),i=d(ni),r=d(G);super(e,i,r)}ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()})}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:e,end:i}=this._container;return e!=null&&e.mode!=="over"&&e.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(Yt("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),P("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[Se([{provide:Ti,useExisting:t}]),ve],ngContentSelectors:nm,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},encapsulation:2,changeDetection:0})}return t})(),Tv=(()=>{class t{_elementRef=d(F);_focusTrapFactory=d(Rl);_focusMonitor=d(Ot);_platform=d(we);_ngZone=d(G);_renderer=d(Te);_interactivityChecker=d(Ea);_doc=d(K);_container=d(Rv,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(e){e=e==="end"?"end":"start",e!==this._position&&(this._isAttached&&this._updatePositionInParent(e),this._position=e,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(e){this._mode=e,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(e){this._disableClose=$t(e)}_disableClose=!1;get autoFocus(){let e=this._autoFocus;return e??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(e){(e==="true"||e==="false"||e==null)&&(e=$t(e)),this._autoFocus=e}_autoFocus;get opened(){return this._opened()}set opened(e){this.toggle($t(e))}_opened=le(!1);_openedVia=null;_animationStarted=new x;_animationEnd=new x;openedChange=new $(!0);_openedStream=this.openedChange.pipe(ue(e=>e),U(()=>{}));openedStart=this._animationStarted.pipe(ue(()=>this.opened),zc(void 0));_closedStream=this.openedChange.pipe(ue(e=>!e),U(()=>{}));closedStart=this._animationStarted.pipe(ue(()=>!this.opened),zc(void 0));_destroyed=new x;onPositionChanged=new $;_content;_modeChanged=new x;_injector=d(B);_changeDetectorRef=d(ye);constructor(){this.openedChange.pipe(fe(this._destroyed)).subscribe(e=>{e?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let e=this._renderer,i=this._elementRef.nativeElement;return[e.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!lt(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),e.listen(i,"transitionend",this._handleTransitionEvent),e.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_takeFocus(){if(!this._focusTrap)return;let e=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":$e(()=>{!this._focusTrap.focusInitialElement()&&typeof e.focus=="function"&&e.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(e){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,e):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let e=this._doc.activeElement;return!!e&&this._elementRef.nativeElement.contains(e)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(e){return this.toggle(!0,e)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(e=!this.opened,i){e&&i&&(this._openedVia=i);let r=this._setOpen(e,!e&&this._isFocusWithinDrawer(),this._openedVia||"program");return e||(this._openedVia=null),r}_setOpen(e,i,r){return e===this.opened?Promise.resolve(e?"open":"close"):(this._opened.set(e),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",e),!e&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(Ce(1)).subscribe(a=>o(a?"open":"close"))}))}_setIsAnimating(e){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",e)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(e){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;e==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=e=>{let i=this._elementRef.nativeElement;e.target===i&&this._ngZone.run(()=>{e.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(e)})};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&Ne(a2,5),i&2){let o;q(o=Q())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(J("align",null)("tabIndex",r.mode!=="side"?"-1":null),Yt("visibility",!r._container&&!r.opened?"hidden":null),P("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:nm,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(me(),h(0,"div",1,0),z(2),p())},dependencies:[Ti],encapsulation:2,changeDetection:0})}return t})(),kv=(()=>{class t{_dir=d(Ke,{optional:!0});_element=d(F);_ngZone=d(G);_changeDetectorRef=d(ye);_animationDisabled=Fe();_transitionsEnabled=!1;_allDrawers;_drawers=new kn;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(e){this._autosize=$t(e)}_autosize=d(g2);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(e){this._backdropOverride=e==null?null:$t(e)}_backdropOverride=null;backdropClick=new $;_start=null;_end=null;_left=null;_right=null;_destroyed=new x;_doCheckSubject=new x;_contentMargins={left:null,right:null};_contentMarginChanges=new x;get scrollable(){return this._userContent||this._content}_injector=d(B);constructor(){let e=d(we),i=d(gn);this._dir?.change.pipe(fe(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(fe(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&e.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(Ge(this._allDrawers),fe(this._destroyed)).subscribe(e=>{this._drawers.reset(e.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(Ge(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(e=>{this._watchDrawerToggle(e),this._watchDrawerPosition(e),this._watchDrawerMode(e)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe(kr(10),fe(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(e=>e.open())}close(){this._drawers.forEach(e=>e.close())}updateContentMargins(){let e=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")e+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();e+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,e-=r}}e=e||null,i=i||null,(e!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:e,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(e){e._animationStarted.pipe(fe(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),e.mode!=="side"&&e.openedChange.pipe(fe(this._drawers.changes)).subscribe(()=>this._setContainerClass(e.opened))}_watchDrawerPosition(e){e.onPositionChanged.pipe(fe(this._drawers.changes)).subscribe(()=>{$e({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(e){e._modeChanged.pipe(fe(Pt(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(e){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";e?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(e=>{e.position=="end"?(this._end!=null,this._end=e):(this._start!=null,this._start=e)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(e=>e&&!e.disableClose&&this._drawerHasBackdrop(e)).forEach(e=>e._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(e){return e!=null&&e.opened}_drawerHasBackdrop(e){return this._backdropOverride==null?!!e&&e.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,tm,5)(o,Tv,5),i&2){let a;q(a=Q())&&(r._content=a.first),q(a=Q())&&(r._allDrawers=a)}},viewQuery:function(i,r){if(i&1&&Ne(tm,5),i&2){let o;q(o=Q())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[Se([{provide:Rv,useExisting:t}])],ngContentSelectors:l2,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(me(s2),k(0,c2,1,2,"div",0),z(1),z(2,1),k(3,d2,2,0,"mat-drawer-content")),i&2&&(R(r.hasBackdrop?0:-1),m(3),R(r._content?-1:3))},dependencies:[tm],styles:[`.mat-drawer-container {
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
`],encapsulation:2,changeDetection:0})}return t})(),Ua=(()=>{class t extends tm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275cmp=I({type:t,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[Se([{provide:Ti,useExisting:t}]),ve],ngContentSelectors:nm,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},encapsulation:2,changeDetection:0})}return t})(),nc=(()=>{class t extends Tv{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(e){this._fixedInViewport=$t(e)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(e){this._fixedTopGap=Ut(e)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(e){this._fixedBottomGap=Ut(e)}_fixedBottomGap=0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275cmp=I({type:t,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(J("tabIndex",r.mode!=="side"?"-1":null)("align",null),Yt("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),P("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[Se([{provide:Tv,useExisting:t}]),ve],ngContentSelectors:nm,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(me(),h(0,"div",1,0),z(2),p())},dependencies:[Ti],encapsulation:2,changeDetection:0})}return t})(),im=(()=>{class t extends kv{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275cmp=I({type:t,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,Ua,5)(o,nc,5),i&2){let a;q(a=Q())&&(r._content=a.first),q(a=Q())&&(r._allDrawers=a)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[Se([{provide:Rv,useExisting:t},{provide:kv,useExisting:t}]),ve],ngContentSelectors:f2,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(me(u2),k(0,m2,1,2,"div",0),z(1),z(2,1),k(3,h2,2,0,"mat-sidenav-content")),i&2&&(R(r.hasBackdrop?0:-1),m(3),R(r._content?-1:3))},dependencies:[Ua],styles:[p2],encapsulation:2,changeDetection:0})}return t})(),_I=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[pn,ae,pn]})}return t})();function bI(t){return Error(`Unable to find icon with the name "${t}"`)}function _2(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function yI(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function wI(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Ai=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},DI=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Ai(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(ot.HTML,r);if(!a)throw wI(r);let s=uo(a);return this._addSvgIconConfig(e,i,new Ai("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Ai(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(ot.HTML,i);if(!o)throw wI(i);let a=uo(o);return this._addSvgIconSetConfig(e,new Ai("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(ot.RESOURCE_URL,e);if(!i)throw yI(e);let r=this._cachedIconsByUrl.get(i);return r?Z(rm(r)):this._loadSvgIconFromConfig(new Ai(e,null)).pipe(et(o=>this._cachedIconsByUrl.set(i,o)),U(o=>rm(o)))}getNamedSvgIcon(e,i=""){let r=CI(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):ss(bI(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Z(rm(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(U(i=>rm(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Z(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe($n(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(ot.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),Z(null)})));return ci(o).pipe(U(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw bI(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(et(i=>e.svgText=i),U(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Z(null):this._fetchIcon(e).pipe(et(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(uo("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(uo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw _2();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(ot.RESOURCE_URL,i);if(!a)throw yI(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(U(c=>uo(c)),Bi(()=>this._inProgressUrlFetches.delete(a)),cs());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(CI(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return v2(o)?new Ai(o.url,null,o.options):new Ai(o,null)}}static \u0275fac=function(i){return new(i||t)(H(ra,8),H(cl),H(K,8),H(Bt))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function rm(t){return t.cloneNode(!0)}function CI(t,n){return t+":"+n}function v2(t){return!!(t.url&&t.options)}var b2=["*"],y2=new y("MAT_ICON_DEFAULT_OPTIONS"),w2=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(K),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),xI=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],C2=xI.map(t=>`[${t}]`).join(", "),D2=/^url\(['"]?#(.*?)['"]?\)$/,om=(()=>{class t{_elementRef=d(F);_iconRegistry=d(DI);_location=d(w2);_errorHandler=d(Bt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=be.EMPTY;constructor(){let e=d(new fn("aria-hidden"),{optional:!0}),i=d(y2,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(C2),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)xI.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(D2):null;if(c){let u=r.get(s);u||(u=[],r.set(s,u)),u.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ce(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(J("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),at(r.color?"mat-"+r.color:""),P("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",ee],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:b2,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2,changeDetection:0})}return t})(),EI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var ic=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=$t(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=$t(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(J("aria-orientation",r.vertical?"vertical":"horizontal"),P("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
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
`],encapsulation:2,changeDetection:0})}return t})(),am=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var SI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[xa,ri,Xf,ae,am]})}return t})();var x2=["mat-internal-form-field",""],E2=["*"],sm=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&P("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:x2,ngContentSelectors:E2,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var I2=["switch"],S2=["*"];function M2(t,n){t&1&&(h(0,"span",11),Wt(),h(1,"svg",13),ie(2,"path",14),p(),h(3,"svg",15),ie(4,"path",16),p()())}var T2=new y("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),lm=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},Av=(()=>{class t{_elementRef=d(F);_focusMonitor=d(Ot);_changeDetectorRef=d(ye);defaults=d(T2);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new lm(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Fe();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new $;toggleChange=new $;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(st).load(ii);let e=d(new fn("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(He).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new lm(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&Ne(I2,5),i&2){let o;q(o=Q())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Mt("id",r.id),J("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),at(r.color?"mat-"+r.color:""),P("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",ee],color:"color",disabled:[2,"disabled","disabled",ee],disableRipple:[2,"disableRipple","disableRipple",ee],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zt(e)],checked:[2,"checked","checked",ee],hideIcon:[2,"hideIcon","hideIcon",ee],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ee]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[Se([{provide:Qf,useExisting:Zt(()=>t),multi:!0},{provide:tc,useExisting:t,multi:!0}]),Ue],ngContentSelectors:S2,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(me(),h(0,"div",1)(1,"button",2,0),W("click",function(){return r._handleClick()}),ie(3,"div",3)(4,"span",4),h(5,"span",5)(6,"span",6)(7,"span",7),ie(8,"span",8),p(),h(9,"span",9),ie(10,"span",10),p(),k(11,M2,5,0,"span",11),p()()(),h(12,"label",12),W("click",function(a){return a.stopPropagation()}),z(13),p()()),i&2){let o=Ct(2);j("labelPosition",r.labelPosition),m(),P("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),j("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),J("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),m(9),j("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),m(),R(r.hideIcon?-1:11),m(),j("for",r.buttonId),J("id",r._labelId)}},dependencies:[ur,sm],styles:[`.mdc-switch {
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
`],encapsulation:2,changeDetection:0})}return t})(),TI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[Av,ae]})}return t})();var R2=["*"];var A2=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],O2=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],N2=new y("MAT_CARD_CONFIG"),$a=(()=>{class t{appearance;constructor(){let e=d(N2,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&P("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:R2,decls:1,vars:0,template:function(i,r){i&1&&(me(),z(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2,changeDetection:0})}return t})(),kI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var Ga=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})(),RI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return t})(),AI=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&P("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),Wa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:O2,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(me(A2),z(0),nt(1,"div",0),z(2,1),gt(),z(3,2))},encapsulation:2,changeDetection:0})}return t})();var OI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var F2=["input"],P2=["formField"],L2=["*"],Nv=class{source;value;constructor(n,e){this.source=n,this.value=e}};var B2=new y("MatRadioGroup"),j2=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})});var V2=(()=>{class t{_elementRef=d(F);_changeDetector=d(ye);_focusMonitor=d(Ot);_radioDispatcher=d(lv);_defaultOptions=d(j2,{optional:!0});_ngZone=d(G);_renderer=d(Te);_uniqueId=d(He).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new $;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=Fe();_injector=d(B);constructor(){d(st).load(ii);let e=d(B2,{optional:!0}),i=d(new fn("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=zt(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new Nv(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_onTouchTargetClick(e){this._onInputInteraction(e),(!this.disabled||this.disabledInteractive)&&this._inputElement?.nativeElement.focus()}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,$e(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===r&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&Ne(F2,5)(P2,7,F),i&2){let o;q(o=Q())&&(r._inputElement=o.first),q(o=Q())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&W("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(J("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),P("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",ee],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zt(e)],checked:[2,"checked","checked",ee],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",ee],required:[2,"required","required",ee],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ee]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:L2,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition"],[1,"mdc-radio"],["aria-hidden","true",1,"mat-mdc-radio-touch-target",3,"click"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mdc-label",3,"for"]],template:function(i,r){i&1&&(me(),h(0,"div",2,0)(2,"div",3)(3,"div",4),W("click",function(a){return r._onTouchTargetClick(a)}),p(),h(4,"input",5,1),W("change",function(a){return r._onInputInteraction(a)}),p(),h(6,"div",6),ie(7,"div",7)(8,"div",8),p(),h(9,"div",9),ie(10,"div",10),p()(),h(11,"label",11),z(12),p()()),i&2&&(j("labelPosition",r.labelPosition),m(2),P("mdc-radio--disabled",r.disabled),m(2),j("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),J("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),m(5),j("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0),m(2),j("for",r.inputId))},dependencies:[ur,sm],styles:[`.mat-mdc-radio-button {
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
`],encapsulation:2,changeDetection:0})}return t})(),NI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ri,V2,ae]})}return t})();var H2=["mat-menu-item",""],z2=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],U2=["mat-icon, [matMenuItemIcon]","*"];function $2(t,n){t&1&&(Wt(),h(0,"svg",2),ie(1,"polygon",3),p())}var G2=["*"];function W2(t,n){if(t&1){let e=it();nt(0,"div",0),Xo("click",function(){De(e);let r=D();return xe(r.closed.emit("click"))})("animationstart",function(r){De(e);let o=D();return xe(o._onAnimationStart(r.animationName))})("animationend",function(r){De(e);let o=D();return xe(o._onAnimationDone(r.animationName))})("animationcancel",function(r){De(e);let o=D();return xe(o._onAnimationDone(r.animationName))}),nt(1,"div",1),z(2),gt()()}if(t&2){let e=D();at(e._classList),P("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Mt("id",e.panelId),J("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var Pv=new y("MAT_MENU_PANEL"),rc=(()=>{class t{_elementRef=d(F);_document=d(K);_focusMonitor=d(Ot);_parentMenu=d(Pv,{optional:!0});_changeDetectorRef=d(ye);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new x;_focused=new x;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(st).load(ii),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&W("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(J("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),P("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",ee],disableRipple:[2,"disableRipple","disableRipple",ee]},exportAs:["matMenuItem"],attrs:H2,ngContentSelectors:U2,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(me(z2),z(0),h(1,"span",0),z(2,1),p(),ie(3,"div",1),k(4,$2,2,0,":svg:svg",2)),i&2&&(m(3),j("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),m(),R(r._triggersSubmenu?4:-1))},dependencies:[ur],encapsulation:2,changeDetection:0})}return t})();var q2=new y("MatMenuContent");var Q2=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),Fv="_mat-menu-enter",cm="_mat-menu-exit",Qa=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(ye);_injector=d(B);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Fe();_allItems;_directDescendantItems=new kn;_classList={};_panelAnimationState="void";_animationDone=new x;_isAnimating=le(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=b({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new $;close=this.closed;panelId=d(He).getId("mat-menu-panel-");constructor(){let e=d(Q2);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new jl(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(Ge(this._directDescendantItems),Ie(e=>Pt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(Ge(this._directDescendantItems),Ie(i=>Pt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:lt(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=$e(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=X(b({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===cm;(i||e===Fv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===Fv||e===cm)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(cm),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?Fv:cm)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(Ge(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,q2,5)(o,rc,5)(o,rc,4),i&2){let a;q(a=Q())&&(r.lazyContent=a.first),q(a=Q())&&(r._allItems=a),q(a=Q())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&Ne(ht,5),i&2){let o;q(o=Q())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&J("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",ee],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:ee(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Se([{provide:Pv,useExisting:t}])],ngContentSelectors:G2,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(me(),Qs(0,W2,3,12,"ng-template"))},styles:[`mat-menu {
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
`],encapsulation:2,changeDetection:0})}return t})(),Y2=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ki(t)}});var qa=new WeakMap,K2=(()=>{class t{_canHaveBackdrop;_element=d(F);_viewContainerRef=d(pt);_menuItemInstance=d(rc,{optional:!0,self:!0});_dir=d(Ke,{optional:!0});_focusMonitor=d(Ot);_ngZone=d(G);_injector=d(B);_scrollStrategy=d(Y2);_changeDetectorRef=d(ye);_animationsDisabled=Fe();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=be.EMPTY;_menuCloseSubscription=be.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(Pv,{optional:!0});this._parentMaterialMenu=i instanceof Qa?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&qa.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=qa.get(i);qa.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Qa&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(fe(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Qa&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ce(1)).subscribe(()=>{i.detach(),qa.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&qa.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Hn(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Qa&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new jn({positionStrategy:po(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[u,f]=[r,o],g=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}g=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:g},{originX:o,originY:l,overlayX:f,overlayY:a,offsetY:g},{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:-g},{originX:o,originY:c,overlayX:f,overlayY:s,offsetY:-g}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Z(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(ue(a=>this._menuOpen&&a!==this._menuItemInstance)):Z();return Pt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Bn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return qa.get(e)===this}_triggerIsAriaDisabled(){return ee(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){su()};static \u0275dir=N({type:t})}return t})(),FI=(()=>{class t extends K2{_cleanupTouchstart;_hoverSubscription=be.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new $;onMenuOpen=this.menuOpened;menuClosed=new $;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Te);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{co(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){lo(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&W("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&J("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[ve]})}return t})();var PI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ri,_n,ae,pn]})}return t})();var dm=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new Lv(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},Lv=class{row;col;constructor(n,e){this.row=n,this.col=e}};var LI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-line",""],["","matLine",""]],hostAttrs:[1,"mat-line"]})}return t})();function BI(t,n,e="mat"){t.changes.pipe(Ge(t)).subscribe(({length:i})=>{oc(n,`${e}-2-line`,!1),oc(n,`${e}-3-line`,!1),oc(n,`${e}-multi-line`,!1),i===2||i===3?oc(n,`${e}-${i}-line`,!0):i>3&&oc(n,`${e}-multi-line`,!0)})}function oc(t,n,e){t.nativeElement.classList.toggle(n,e)}var Bv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[ae]})}return t})();var jI=["*"],X2=[[["","mat-grid-avatar",""],["","matGridAvatar",""]],[["","mat-line",""],["","matLine",""]],"*"],J2=["[mat-grid-avatar], [matGridAvatar]","[mat-line], [matLine]","*"],eB=`.mat-grid-list {
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
`,VI=new y("MAT_GRID_LIST"),sc=(()=>{class t{_element=d(F);_gridList=d(VI,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round(Ut(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round(Ut(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&J("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:jI,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(me(),nt(0,"div",0),z(1),gt())},styles:[`.mat-grid-list {
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
`],encapsulation:2,changeDetection:0})}return t})(),um=(()=>{class t{_element=d(F);_lines;constructor(){}ngAfterContentInit(){BI(this._lines,this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-grid-tile-header"],["mat-grid-tile-footer"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,LI,5),i&2){let a;q(a=Q())&&(r._lines=a)}},ngContentSelectors:J2,decls:4,vars:0,consts:[[1,"mat-grid-list-text"]],template:function(i,r){i&1&&(me(X2),z(0),nt(1,"div",0),z(2,1),gt(),z(3,2))},encapsulation:2,changeDetection:0})}return t})();var fm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-grid-tile-header"]],hostAttrs:[1,"mat-grid-tile-header"]})}return t})();var tB=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,ac=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=HI(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":yo(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),a=this._direction==="rtl"?"right":"left";n._setStyle(a,this.getTilePosition(o,e)),n._setStyle("width",yo(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},jv=class extends ac{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=HI(this.fixedRowHeight),tB.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",yo(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",yo(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},Vv=class extends ac{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",yo(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",yo(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},Hv=class extends ac{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",yo(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function yo(t){return`calc(${t})`}function HI(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var nB="fit",mm=(()=>{class t{_element=d(F);_dir=d(Ke,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round(Ut(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===nB?this._tileStyler=new Hv:e&&e.indexOf(":")>-1?this._tileStyler=new Vv(e):this._tileStyler=new jv(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new dm);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,a)=>{let s=e.positions[a];this._tileStyler.setStyle(o,s.row,s.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&Ye(o,sc,5),i&2){let a;q(a=Q())&&(r._tiles=a)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&J("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[Se([{provide:VI,useExisting:t}])],ngContentSelectors:jI,decls:2,vars:0,template:function(i,r){i&1&&(me(),nt(0,"div"),z(1),gt())},styles:[eB],encapsulation:2,changeDetection:0})}return t})(),zI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[Bv,ae,Bv]})}return t})();var $I=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({})}return t})();var GI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[$I,ae]})}return t})();function iB(t,n){}var gr=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Uv=(()=>{class t extends sr{_elementRef=d(F);_focusTrapFactory=d(Rl);_config;_interactivityChecker=d(Ea);_ngZone=d(G);_focusMonitor=d(Ot);_renderer=d(Te);_changeDetectorRef=d(ye);_injector=d(B);_platform=d(we);_document=d(K);_portalOutlet;_focusTrapped=new x;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(gr,{optional:!0})||new gr,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||$e(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Ml(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Ml();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Ml()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=I({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Ne(lr,7),i&2){let o;q(o=Q())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&J("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[ve],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&ze(0,iB,0,0,"ng-template",0)},dependencies:[lr],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),lc=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new x;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!lt(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},rB=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ka(t)}}),oB=new y("DialogData"),aB=new y("DefaultDialogConfig");function sB(t){let n=le(t),e=new $;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var $v=(()=>{class t{_injector=d(B);_defaultOptions=d(aB,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Ef);_idGenerator=d(He);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;_ariaHiddenElements=new Map;_scrollStrategy=d(rB);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=wn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ge(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new gr;i=b(b({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=Hn(this._injector,o),s=new lc(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Ce(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){zv(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){zv(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),zv(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new jn({positionStrategy:e.positionStrategy||dr().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:gr,useValue:r},{provide:lc,useValue:i},{provide:Ma,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=Uv;let l=new Ln(s,r.viewContainerRef,B.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof ht){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=b(b({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Bn(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new Ln(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:oB,useValue:e.data},{provide:lc,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Ke,null,{optional:!0}))&&s.push({provide:Ke,useValue:sB(e.direction)}),B.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function zv(t,n){let e=t.length;for(;e--;)n(t[e])}var WI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({providers:[$v],imports:[_n,cr,Ol,cr]})}return t})();function lB(t,n){}var pm=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Gv="mdc-dialog--open",qI="mdc-dialog--opening",QI="mdc-dialog--closing",cB=150,dB=75,uB=(()=>{class t extends Uv{_animationStateChanged=new $;_animationsEnabled=!Fe();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?KI(this._config.enterAnimationDuration)??cB:0;_exitAnimationDuration=this._animationsEnabled?KI(this._config.exitAnimationDuration)??dB:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(YI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(qI,Gv)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Gv),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Gv),this._animationsEnabled?(this._hostElement.style.setProperty(YI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(QI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(qI,QI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275cmp=I({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Mt("id",r._config.id),J("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),P("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[ve],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(h(0,"div",0)(1,"div",1),ze(2,lB,0,0,"ng-template",2),p()())},dependencies:[lr],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2})}return t})(),YI="--mat-dialog-transition-duration";function KI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Ut(t.substring(0,t.length-2)):t.endsWith("s")?Ut(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var hm=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(hm||{}),wo=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new yn(1);_beforeClosed=new yn(1);_result;_closeFallbackTimeout;_state=hm.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ue(r=>r.state==="opened"),Ce(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ue(r=>r.state==="closed"),Ce(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Pt(this.backdropClick(),this.keydownEvents().pipe(ue(r=>r.keyCode===27&&!this.disableClose&&!lt(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),ZI(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(ue(i=>i.state==="closing"),Ce(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=hm.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=hm.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function ZI(t,n,e){return t._closeInteractionType=n,t.close(e)}var fB=new y("MatMdcDialogData"),mB=new y("mat-mdc-dialog-default-options"),hB=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(B);return()=>ka(t)}}),cc=(()=>{class t{_defaultOptions=d(mB,{optional:!0});_scrollStrategy=d(hB);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(He);_injector=d(B);_dialog=d($v);_animationsDisabled=Fe();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;dialogConfigClass=pm;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=wn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ge(void 0)));constructor(){this._dialogRefConstructor=wo,this._dialogContainerType=uB,this._dialogDataToken=fB}open(e,i){let r;i=b(b({},this._defaultOptions||new pm),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,X(b({},i),{positionStrategy:dr(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:gr,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),XI=(()=>{class t{dialogRef=d(wo,{optional:!0});_elementRef=d(F);_dialog=d(cc);ariaLabel;type="button";dialogResult;_matDialogClose;constructor(){}ngOnInit(){this.dialogRef||(this.dialogRef=iS(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose||e._matDialogCloseResult;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){ZI(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&W("click",function(a){return r._onButtonClick(a)}),i&2&&J("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[Ue]})}return t})(),JI=(()=>{class t{_dialogRef=d(wo,{optional:!0});_elementRef=d(F);_dialog=d(cc);constructor(){}ngOnInit(){this._dialogRef||(this._dialogRef=iS(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t})}return t})(),eS=(()=>{class t extends JI{id=d(He).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&Mt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[ve]})}return t})(),tS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[yg([Ti])]})}return t})(),nS=(()=>{class t extends JI{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qe(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&P("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[ve]})}return t})();function iS(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var rS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=T({type:t});static \u0275inj=M({providers:[cc],imports:[WI,_n,cr,ae]})}return t})();var Kt=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=T({type:t});static \u0275inj=M({imports:[vE,mI,hI,gI,_I,EI,SI,_o,TI,OI,NI,Jf,PI,zI,GI,am,rS]})};var _r=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-page-footer"]],decls:7,vars:3,consts:[[1,"page-footer","text-xs","flex","items-center","flex-row","flex-wrap"],["color","primary",1,"page-footer"],[1,"flex","flex-1","justify-end","text-xl"],["href","https://boardgamegeek.com/user/Vortilion","target","_blank",1,"text-blue-700"]],template:function(e,i){e&1&&(h(0,"div",0)(1,"mat-toolbar",1)(2,"div",2),v(3),Di(4,"transloco"),h(5,"a",3),v(6,"Vortilion"),p()()()()),e&2&&(m(3),E(" ",xi(4,1,"creator-prefix"),"\xA0"))},dependencies:[Kt,em,La],styles:[".page-footer[_ngcontent-%COMP%]{color:var(--mat-sys-primary);background:var(--bg-color)}"]})};var pB=t=>({active:t}),oS=t=>({color:t});function gB(t,n){if(t&1){let e=it();h(0,"button",3),W("click",function(){let r=De(e).$implicit,o=D();return xe(o.changeLanguage(r))}),h(1,"mat-icon",4),v(2," language "),p(),h(3,"span",4),v(4),Di(5,"transloco"),p()()}if(t&2){let e=n.$implicit,i=D();j("ngClass",Ci(6,pB,i.activeLang===e)),m(),j("ngStyle",Ci(8,oS,i.activeLang===e?"rgb(255 143 0)":"")),m(2),j("ngStyle",Ci(10,oS,i.activeLang===e?"rgb(255 143 0)":"")),m(),E(" ",xi(5,4,e+"-language-label")," ")}}var Ya=class t{translocoService=d(Pa);activeLang;availableLangs;ngOnInit(){let n=gE();this.availableLangs=this.translocoService.getAvailableLangs(),n&&this.translocoService.isLang(n)?(this.activeLang=n,this.translocoService.setActiveLang(this.activeLang)):this.activeLang=this.translocoService.getDefaultLang()}changeLanguage(n){this.translocoService.setActiveLang(n),this.activeLang=this.translocoService.getActiveLang()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-language-selector"]],decls:10,vars:4,consts:[["menu","matMenu"],["mat-icon-button","","aria-label","Language Selection",1,"example-icon","favorite-icon",3,"matMenuTriggerFor"],["mat-menu-item","",3,"ngClass"],["mat-menu-item","",3,"click","ngClass"],[3,"ngStyle"]],template:function(e,i){if(e&1&&(h(0,"button",1)(1,"mat-icon"),v(2,"translate"),p()(),h(3,"span"),v(4),Di(5,"transloco"),p(),h(6,"mat-menu",null,0),It(8,gB,6,12,"button",2,un),p()),e&2){let r=Ct(7);j("matMenuTriggerFor",r),m(4),V(xi(5,2,i.activeLang+"-language-label")),m(4),St(i.availableLangs)}},dependencies:[Du,Bg,jg,Kt,om,go,Qa,rc,FI,La],encapsulation:2})};function _B(t,n){if(t&1&&(h(0,"a",3)(1,"mat-icon"),v(2,"apps"),p()()),t&2){let e=D();j("routerLink",e.dashboardRoute)}}function vB(t,n){if(t&1&&(h(0,"span"),v(1),Di(2,"transloco"),p()),t&2){let e=D();m(),V(xi(2,1,e.titlePrefix+"app-title"))}}function bB(t,n){if(t&1&&(h(0,"span",5),v(1),Di(2,"transloco"),p()),t&2){let e=D();m(),V(xi(2,1,e.titlePrefix+"app-title-short"))}}var Ka=class t{responsive=d(Fn);sidebarHandle;dashboardRoute;titlePrefix="";isXSmall;ngOnInit(){this.responsive.observe(ar.XSmall).subscribe(n=>{n.matches?this.isXSmall=!0:this.isXSmall=!1})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-page-header"]],inputs:{sidebarHandle:"sidebarHandle",dashboardRoute:"dashboardRoute",titlePrefix:"titlePrefix"},decls:11,vars:5,consts:[[1,"page-header"],[1,"main-toolbar","toolbar"],["type","button","mat-icon-button","",3,"click","hidden"],["mat-icon-button","","aria-label","Randomizers dashboard",3,"routerLink"],[1,"app-name","text-xl"],[1,"text-xl"],[1,"spacer"]],template:function(e,i){e&1&&(h(0,"div",0)(1,"mat-toolbar",1)(2,"button",2),W("click",function(){return i.sidebarHandle.toggle()}),h(3,"mat-icon"),v(4,"menu"),p()(),k(5,_B,3,1,"a",3),h(6,"span",4),k(7,vB,3,3,"span")(8,bB,3,3,"span",5),p(),ie(9,"span",6)(10,"app-language-selector"),p()()),e&2&&(m(),P("is-mobile",i.isXSmall),m(),j("hidden",!i.isXSmall),m(3),R(i.dashboardRoute?5:-1),m(2),R(i.isXSmall?8:7))},dependencies:[Kt,em,om,go,Ya,va,La],styles:[".is-mobile[_ngcontent-%COMP%]{position:fixed;z-index:2}.app-name[_ngcontent-%COMP%]{margin:0 0 0 8px}.spacer[_ngcontent-%COMP%]{flex:1 1 auto}.mat-toolbar[_ngcontent-%COMP%]{color:var(--mat-sys-primary);background:var(--bg-color)}"]})};var Za=class t{get(n){if(!this.isStorageAvailable())return null;let e=this.getString(n);if(e===null)return null;try{return JSON.parse(e)}catch{return e}}set(n,e){if(!this.isStorageAvailable())return!1;try{let i=typeof e=="string"?e:JSON.stringify(e);return globalThis.localStorage.setItem(n,i),!0}catch{return!1}}delete(n){if(!this.isStorageAvailable())return!1;try{return globalThis.localStorage.removeItem(n),!0}catch{return!1}}clear(){if(!this.isStorageAvailable())return!1;try{return globalThis.localStorage.clear(),!0}catch{return!1}}getNumber(n){let e=this.getString(n);if(e===null)return null;let i=Number(e);return Number.isFinite(i)?i:null}setNumber(n,e){this.setString(n,String(e))}getString(n){return this.isStorageAvailable()?globalThis.localStorage.getItem(n):null}setString(n,e){this.isStorageAvailable()&&globalThis.localStorage.setItem(n,e)}isStorageAvailable(){return typeof globalThis>"u"||!("localStorage"in globalThis)?!1:globalThis.localStorage!==void 0&&globalThis.localStorage!==null}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var yB=t=>({"w-full":t});function wB(t,n){if(t&1&&(h(0,"mat-option",10),v(1),h(2,"span"),v(3),p()()),t&2){let e=n.$implicit,i=D().$implicit;j("value",e.value),m(),E(" ",e.label," "),m(2),V(i("players-label"))}}function CB(t,n){if(t&1&&(h(0,"li",21)(1,"span"),v(2),p()()),t&2){let e=n.$implicit;m(2),V(e.title)}}function DB(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"ul",20),It(7,CB,3,1,"li",21,un),p()()),t&2){let e=D().$implicit,i=D();m(4),V(e("neutral-buildings-label")),m(3),St(i.randomNeutralBuildings)}}function xB(t,n){if(t&1&&(h(0,"div"),ie(1,"img",22),p()),t&2){let e=n.$implicit;m(),j("src",e.sides[0].image,Ko)}}function EB(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"div",20),It(7,xB,2,1,"div",null,un),p()()),t&2){let e=D().$implicit,i=D();m(4),V(e("station-masters-label")),m(3),St(i.randomStationMasters)}}function IB(t,n){if(t&1&&(h(0,"span",17)(1,"span",23),v(2),p()()),t&2){let e=n.$implicit;m(2),eo("",e.title,"",e.sides[0].title," ")}}function SB(t,n){if(t&1&&(h(0,"span",17)(1,"span",23),v(2),p()()),t&2){let e=n.$implicit;m(2),eo("",e.title,": ",e.sides[0].title," ")}}function MB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step41")," ")}}function TB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step42")," ")}}function kB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step43")," ")}}function RB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step71")," ")}}function AB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step72")," ")}}function OB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step73")," ")}}function NB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step83")," ")}}function FB(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step84")," ")}}function PB(t,n){if(t&1){let e=it();yt(0),h(1,"div",2),ie(2,"app-page-header",3),h(3,"mat-sidenav-container",4)(4,"mat-sidenav",5,0)(6,"div",6)(7,"h2"),v(8),p(),ie(9,"mat-divider",7),h(10,"h3"),v(11),p(),h(12,"mat-form-field",8)(13,"mat-label"),v(14),p(),h(15,"mat-select",9),W("selectionChange",function(r){De(e);let o=D();return xe(o.onPlayerCountChange(r))}),It(16,wB,4,3,"mat-option",10,un),p()()()(),h(18,"mat-sidenav-content")(19,"div",11)(20,"div",12)(21,"div",13)(22,"button",14),W("click",function(){De(e);let r=D();return xe(r.randomizeSetup())}),h(23,"span"),v(24),p()()(),h(25,"mat-grid-list",15),k(26,DB,9,1,"mat-grid-tile"),k(27,EB,9,1,"mat-grid-tile"),h(28,"mat-grid-tile")(29,"mat-grid-tile-header")(30,"h3")(31,"span"),v(32),p(),v(33,": "),p()(),h(34,"div",16),It(35,IB,3,2,"span",17,un),p()(),h(37,"mat-grid-tile")(38,"mat-grid-tile-header")(39,"h3")(40,"span"),v(41),p(),v(42,": "),p()(),h(43,"div",16),It(44,SB,3,2,"span",17,un),p()()(),h(46,"mat-card")(47,"mat-card-header")(48,"h3")(49,"span"),v(50),p()()(),h(51,"mat-card-content")(52,"ol",18)(53,"li"),v(54),h(55,"ul",19)(56,"li"),v(57),p(),h(58,"li"),v(59),p(),h(60,"li"),v(61),p()()(),h(62,"li"),v(63),h(64,"ul",19)(65,"li"),v(66),p(),h(67,"li"),v(68),p(),h(69,"li"),v(70),p()()(),h(71,"li"),v(72),h(73,"ul",19)(74,"li"),v(75),p(),h(76,"li"),v(77),p(),h(78,"li"),v(79),p()()(),h(80,"li"),v(81),h(82,"ul",19),k(83,MB,2,1,"li"),k(84,TB,2,1,"li"),k(85,kB,2,1,"li"),h(86,"li"),v(87),p()()(),h(88,"li"),v(89),p(),h(90,"li")(91,"p"),v(92),p(),h(93,"p"),v(94),p(),h(95,"p"),v(96),p()(),h(97,"li"),v(98),h(99,"ul",19),k(100,RB,2,1,"li"),k(101,AB,2,1,"li"),k(102,OB,2,1,"li"),p()(),h(103,"li"),v(104),h(105,"ul",19)(106,"li"),v(107),p(),h(108,"li"),v(109),p(),k(110,NB,2,1,"li"),k(111,FB,2,1,"li"),p()()()()()()()()(),ie(112,"app-page-footer"),p(),wt()}if(t&2){let e=n.$implicit,i=Ct(5),r=D();m(),P("is-mobile",r.isXSmall),m(),j("sidebarHandle",i)("dashboardRoute","/randomizers")("titlePrefix","argentina."),m(),Yt("padding-top",r.isXSmall?56:0,"px"),m(),j("mode",r.isXSmall?"over":"side")("fixedInViewport",r.isXSmall)("opened",r.isXSmall?"false":"opened"),m(4),E("",e("options-label"),":"),m(3),E("",e("player-count-label"),":"),m(3),V(e("player-count-select-label")),m(),j("value",r.playerCount),m(),St(r.playerCountList),m(6),at(Ci(53,yB,r.isXSmall)),m(2),V(e("btn-setup-label")),m(),j("cols",r.isMax1280?1:2),m(),R(r.randomNeutralBuildings.length>0?26:-1),m(),R(r.randomStationMasters&&r.randomStationMasters.length>0?27:-1),m(5),V(e("player-buildings-label")),m(3),St(r.randomPlayerBuildings),m(6),V(e("cities-label")),m(3),St(r.randomCities),m(6),V(e("further-setup-steps-label")),m(4),E(" ",e("further-steps-step1")," "),m(3),E(" ",e("further-steps-step1a")," "),m(2),E(" ",e("further-steps-step1b")," "),m(2),E(" ",e("further-steps-step-1c")," "),m(2),E(" ",e("further-steps-step2")," "),m(3),E(" ",e("further-steps-step21")," "),m(2),E(" ",e("further-steps-step22")," "),m(2),E(" ",e("further-steps-step23")," "),m(2),E(" ",e("further-steps-step3")," "),m(3),E(" ",e("further-steps-step31")," "),m(2),E(" ",e("further-steps-step32")," "),m(2),E(" ",e("further-steps-step33")," "),m(2),E(" ",e("further-steps-step4")," "),m(2),R(r.playerCount===2?83:-1),m(),R(r.playerCount===3?84:-1),m(),R(r.playerCount===4?85:-1),m(2),E(" ",e("further-steps-step4b")," "),m(2),V(e("further-steps-step5")),m(3),V(e("further-steps-step6")),m(2),V(e("further-steps-step6a")),m(2),V(e("further-steps-step6b")),m(2),E(" ",e("further-steps-step7")," "),m(2),R(r.playerCount===2?100:-1),m(),R(r.playerCount===3?101:-1),m(),R(r.playerCount===4?102:-1),m(2),E(" ",e("further-steps-step8")," "),m(3),V(e("further-steps-step81")),m(2),V(e("further-steps-step82")),m(),R(r.playerCount>=3?110:-1),m(),R(r.playerCount>3?111:-1)}}var gm=class t{applicationConfigService=d(Of);responsive=d(Fn);storageService=d(Za);randomNeutralBuildings;randomPlayerBuildings;randomStationMasters;randomCities;playerCount;playerCountList;isXSmall;isMax1280;ngOnInit(){this.playerCount=2,this.playerCountList=[{label:"2",value:2},{label:"3",value:3},{label:"4",value:4}],this.responsive.observe(ar.XSmall).subscribe(e=>{e.matches?this.isXSmall=!0:this.isXSmall=!1}),this.responsive.observe("(max-width: 1280px)").subscribe(e=>{e.matches?this.isMax1280=!0:this.isMax1280=!1});let n=this.storageService.getNumber("rar-playerCount");n!==null?this.emitPlayerCount(n):this.storageService.setNumber("rar-playerCount",2),this.applicationConfigService.playerCount.subscribe(e=>{this.playerCount=e}),this.randomizeSetup()}emitPlayerCount(n){this.applicationConfigService.playerCount.emit(n)}onPlayerCountChange(n){let e=Number(n.value);this.storageService.setNumber("rar-playerCount",e),this.emitPlayerCount(e)}randomizeSetup(){this.randomNeutralBuildings=this.applicationConfigService.getRandomNeutralBuildingOrder(),this.randomStationMasters=this.applicationConfigService.getRandomStationMasters(),this.randomPlayerBuildings=this.applicationConfigService.getRandomPlayerBuildings(),this.randomCities=this.applicationConfigService.getRandomCities()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-argentina"]],decls:1,vars:1,consts:[["sidenav",""],[4,"transloco","translocoRead"],[1,"argentina-component","flex","flex-col"],[3,"sidebarHandle","dashboardRoute","titlePrefix"],[1,"sidenav-container","flex-1"],["fixedTopGap","56",1,"sidenav",3,"mode","fixedInViewport","opened"],[1,"sidenav__inner","p-4"],[1,"divider","mb-4"],["appearance","fill"],[3,"selectionChange","value"],[3,"value"],[1,"sidenav-content","flex","flex-col","min-h-full"],[1,"flex-1","px-2","xSmall:px-10","py-5"],[1,"mb-4"],["mat-flat-button","","color","primary",3,"click"],["rowHeight","180px","gutterSize","5px",3,"cols"],[1,"flex","flex-wrap"],[1,"px-2","text-2xl","xSmall:text-xl","flex-1"],[1,"pl-4","list-decimal"],[1,"list-disc","pl-8","text-gray-400"],[1,"flex"],[1,"px-2","text-2xl","xSmall:text-xl"],["alt","Station Master","width","100",1,"px-1","xSmall:px-2","md:px-4",3,"src"],[1,"whitespace-nowrap"]],template:function(e,i){e&1&&ze(0,PB,113,55,"ng-container",1),e&2&&j("translocoRead","argentina")},dependencies:[Kt,nc,im,Ua,ic,oi,$a,Ga,Wa,Ri,hr,za,ai,mm,sc,um,fm,Ka,_r,mr],styles:[`.is-mobile[_ngcontent-%COMP%]   .sidenav-container[_ngcontent-%COMP%]{flex-shrink:0;flex-grow:1;flex-basis:auto}.divider[_ngcontent-%COMP%]{margin-bottom:16px!important}
`]})};function LB(t,n){if(t&1&&(yt(0),h(1,"div",1)(2,"header",2)(3,"div",3)(4,"div")(5,"p",4),v(6),p(),h(7,"h1",5),v(8),p(),h(9,"p",6),v(10),p()(),ie(11,"app-language-selector"),p()(),h(12,"main",7)(13,"div",8)(14,"mat-card",9)(15,"mat-card-header")(16,"mat-card-title"),v(17),p(),h(18,"mat-card-subtitle"),v(19),p()(),h(20,"mat-card-content")(21,"p"),v(22),p()(),h(23,"mat-card-actions")(24,"a",10),v(25),p()()(),h(26,"mat-card",9)(27,"mat-card-header")(28,"mat-card-title"),v(29),p(),h(30,"mat-card-subtitle"),v(31),p()(),h(32,"mat-card-content")(33,"p"),v(34),p()(),h(35,"mat-card-actions")(36,"a",11),v(37),p()()()()(),ie(38,"app-page-footer"),p(),wt()),t&2){let e=n.$implicit;m(6),V(e("eyebrow")),m(2),V(e("title")),m(2),E(" ",e("subtitle")," "),m(7),V(e("argentina.title")),m(2),V(e("argentina.subtitle")),m(3),V(e("argentina.description")),m(3),E(" ",e("argentina.action")," "),m(4),V(e("second-edition.title")),m(2),V(e("second-edition.subtitle")),m(3),V(e("second-edition.description")),m(3),E(" ",e("second-edition.action")," ")}}var _m=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-randomizers-dashboard"]],decls:1,vars:1,consts:[[4,"transloco","translocoRead"],[1,"dashboard","flex","min-h-screen","flex-col"],[1,"dashboard__hero","px-4","py-6","xSmall:px-8","xSmall:py-8"],[1,"dashboard__topbar","mb-8","flex","items-start","justify-between","gap-4"],[1,"dashboard__eyebrow","mb-2"],[1,"dashboard__title","text-4xl","xSmall:text-5xl"],[1,"dashboard__subtitle","mt-3","max-w-3xl","text-base","xSmall:text-lg"],[1,"flex-1","px-4","pb-8","xSmall:px-8"],[1,"grid","gap-4","lg:grid-cols-2"],[1,"dashboard-card"],["mat-flat-button","","color","primary","routerLink","/argentina"],["mat-flat-button","","color","primary","routerLink","/2nd-edition"]],template:function(e,i){e&1&&ze(0,LB,39,11,"ng-container",0),e&2&&j("translocoRead","dashboard")},dependencies:[va,mr,Ya,Kt,oi,$a,AI,Ga,Wa,RI,kI,_r],styles:[".dashboard[_ngcontent-%COMP%]{background:radial-gradient(circle at top left,rgba(255,235,205,.85),transparent 38%),linear-gradient(180deg,#f6efe2,#ebe2cf 48%,#dfd3bc)}.dashboard__hero[_ngcontent-%COMP%]{border-bottom:1px solid rgba(109,76,65,.2)}.dashboard__eyebrow[_ngcontent-%COMP%]{color:#8d6e63;font-size:.85rem;font-weight:700;letter-spacing:.16em;text-transform:uppercase}.dashboard__title[_ngcontent-%COMP%]{color:#3e2723;font-weight:700;line-height:1}.dashboard__subtitle[_ngcontent-%COMP%]{color:#5d4037}.dashboard-card[_ngcontent-%COMP%]{background:#fffcf7e0;border:1px solid rgba(109,76,65,.14);box-shadow:0 18px 40px #3e27231a}"]})};var vm=class t{useVariant=new $;useRailsToTheNorth=new $;playerCount=new $;neutralBuildings=[{title:"A",sides:[{title:"front"}]},{title:"B",sides:[{title:"front"}]},{title:"C",sides:[{title:"front"}]},{title:"D",sides:[{title:"front"}]},{title:"E",sides:[{title:"front"}]},{title:"F",sides:[{title:"front"}]},{title:"G",sides:[{title:"front"}]}];playerBuildings=[{title:"1",sides:[{title:"a"},{title:"b"}]},{title:"2",sides:[{title:"a"},{title:"b"}]},{title:"3",sides:[{title:"a"},{title:"b"}]},{title:"4",sides:[{title:"a"},{title:"b"}]},{title:"5",sides:[{title:"a"},{title:"b"}]},{title:"6",sides:[{title:"a"},{title:"b"}]},{title:"7",sides:[{title:"a"},{title:"b"}]},{title:"8",sides:[{title:"a"},{title:"b"}]},{title:"9",sides:[{title:"a"},{title:"b"}]},{title:"10",sides:[{title:"a"},{title:"b"}]},{title:"11",sides:[{title:"a"},{title:"b"}]},{title:"12",sides:[{title:"a"},{title:"b"}]}];stationMasters=[{title:"1",sides:[{title:"front",image:"img/second-edition/station-master-01.png"}]},{title:"2",sides:[{title:"front",image:"img/second-edition/station-master-02.png"}]},{title:"3",sides:[{title:"front",image:"img/second-edition/station-master-03.png"}]},{title:"4",sides:[{title:"front",image:"img/second-edition/station-master-04.png"}]},{title:"5",sides:[{title:"front",image:"img/second-edition/station-master-05.png"}]},{title:"6",sides:[{title:"front",image:"img/second-edition/station-master-06.png"}]},{title:"7",sides:[{title:"front",image:"img/second-edition/station-master-07.png"}]},{title:"8",sides:[{title:"front",image:"img/second-edition/station-master-08.png"}]},{title:"9",sides:[{title:"front",image:"img/second-edition/station-master-09.png"}]}];getRandomNeutralBuildingOrder(){return this.shuffleArray(this.neutralBuildings)}getRandomStationMasters(){let n=[],e=this.shuffleArray(this.stationMasters);for(let i=0;i<5;i+=1)n.push(e.pop());return n}getRandomPlayerBuildings(){let n=JSON.parse(JSON.stringify(this.playerBuildings));return n.forEach(e=>{e.sides.splice(Math.floor(Math.random()*e.sides.length),1)}),n}shuffleArray(n){let e=n.slice();for(let i=e.length-1;i>0;i-=1){let r=Math.floor(Math.random()*(i+1)),o=e[i];e[i]=e[r],e[r]=o}return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};function BB(t,n){if(t&1&&(yt(0),h(1,"h2",1),v(2),p(),h(3,"mat-dialog-content"),v(4),p(),h(5,"mat-dialog-actions")(6,"button",2),v(7,"Ok"),p()(),wt()),t&2){let e=n.$implicit;m(2),E(" ",e("title")," "),m(2),E(" ",e("content")," ")}}var bm=class t{dialogRef=d(wo);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-variant-warning-dialog"]],decls:1,vars:1,consts:[[4,"transloco","translocoRead"],["mat-dialog-title",""],["mat-button","","mat-dialog-close",""]],template:function(e,i){e&1&&ze(0,BB,8,2,"ng-container",0),e&2&&j("translocoRead","second-edition.modals.variant-warning")},dependencies:[_o,oi,nS,XI,eS,tS,mr],encapsulation:2,changeDetection:0})};var jB=t=>({"w-full":t}),VB=(t,n)=>n.value;function HB(t,n){if(t&1&&(h(0,"mat-option",10),v(1),h(2,"span"),v(3),p()()),t&2){let e=n.$implicit,i=D().$implicit;j("value",e.value),m(),E(" ",e.label," "),m(2),V(i("players-label"))}}function zB(t,n){if(t&1&&(h(0,"li",26)(1,"span"),v(2),p()()),t&2){let e=n.$implicit;m(2),V(e.title)}}function UB(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"ul",25),It(7,zB,3,1,"li",26,Ks),p()()),t&2){let e=D().$implicit,i=D();m(4),V(e("neutral-buildings-label")),m(3),St(i.randomNeutralBuildings)}}function $B(t,n){if(t&1&&(h(0,"div"),ie(1,"img",27),p()),t&2){let e=n.$implicit;m(),j("src",e.sides[0].image,Ko)}}function GB(t,n){if(t&1&&(h(0,"mat-grid-tile")(1,"mat-grid-tile-header")(2,"h3")(3,"span"),v(4),p(),v(5,": "),p()(),h(6,"div",25),It(7,$B,2,1,"div",null,Ks),p()()),t&2){let e=D().$implicit,i=D();m(4),V(e("station-masters-label")),m(3),St(i.randomStationMasters)}}function WB(t,n){if(t&1&&(h(0,"span",22)(1,"span",28),v(2),p()()),t&2){let e=n.$implicit;m(2),eo("",e.title,"",e.sides[0].title," ")}}function qB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),V(e("further-steps-step4_simmental"))}}function QB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),V(e("further-steps-step4_brahman"))}}function YB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D().$implicit;m(),V(e("further-steps-step4"))}}function KB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),V(e("further-steps-step41_simmental_brahman"))}}function ZB(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;E(" ",e("further-steps-step41")," ")}}function XB(t,n){if(t&1&&(h(0,"li"),k(1,KB,2,1,"ng-container")(2,ZB,1,1),p()),t&2){let e=D(2);m(),R(e.useSimmental||e.useBrahman?1:2)}}function JB(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),V(e("further-steps-step42_simmental_brahman"))}}function ej(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;E(" ",e("further-steps-step42")," ")}}function tj(t,n){if(t&1&&(h(0,"li"),k(1,JB,2,1,"ng-container")(2,ej,1,1),p()),t&2){let e=D(2);m(),R(e.useSimmental||e.useBrahman?1:2)}}function nj(t,n){if(t&1&&(yt(0),v(1),wt()),t&2){let e=D(2).$implicit;m(),V(e("further-steps-step43_simmental_brahman"))}}function ij(t,n){if(t&1&&v(0),t&2){let e=D(2).$implicit;E(" ",e("further-steps-step43")," ")}}function rj(t,n){if(t&1&&(h(0,"li"),k(1,nj,2,1,"ng-container")(2,ij,1,1),p()),t&2){let e=D(2);m(),R(e.useSimmental||e.useBrahman?1:2)}}function oj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-simmental2")," ")}}function aj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-simmental3")," ")}}function sj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-brahman")," ")}}function lj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step5")," ")}}function cj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step63")," ")}}function dj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step64")," ")}}function uj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step7")," ")}}function fj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step8")," ")}}function mj(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),h(7,"li"),v(8),p()()()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step9")," "),m(3),V(e("further-steps-step91")),m(2),V(e("further-steps-step92")),m(2),V(e("further-steps-step93"))}}function hj(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),h(7,"li"),v(8),p(),h(9,"li"),v(10),p()()()),t&2){let e=D().$implicit;m(),E(" ",e("further-steps-step10")," "),m(3),V(e("further-steps-step101")),m(2),V(e("further-steps-step102")),m(2),V(e("further-steps-step103")),m(2),V(e("further-steps-step104"))}}function pj(t,n){if(t&1&&(h(0,"li"),v(1),p()),t&2){let e=D(2).$implicit;m(),E(" ",e("further-steps-step11_3")," ")}}function gj(t,n){if(t&1&&(h(0,"li"),v(1),h(2,"ul",24)(3,"li"),v(4),p(),h(5,"li"),v(6),p(),k(7,pj,2,1,"li"),p()()),t&2){let e=D().$implicit,i=D();m(),E(" ",e("further-steps-step11")," "),m(3),V(e("further-steps-step11_1")),m(2),V(e("further-steps-step11_2")),m(),R(i.useSimmental?7:-1)}}function _j(t,n){if(t&1){let e=it();yt(0),h(1,"div",2),ie(2,"app-page-header",3),h(3,"mat-sidenav-container",4)(4,"mat-sidenav",5,0)(6,"div",6)(7,"h2"),v(8),p(),ie(9,"mat-divider",7),h(10,"h3"),v(11),p(),h(12,"mat-form-field",8)(13,"mat-label"),v(14),p(),h(15,"mat-select",9),W("selectionChange",function(r){De(e);let o=D();return xe(o.onPlayerCountChange(r))}),It(16,HB,4,3,"mat-option",10,VB),p()(),h(18,"h3"),v(19),p(),h(20,"div",11)(21,"mat-slide-toggle",12),Js("ngModelChange",function(r){De(e);let o=D();return _u(o.useSimmental,r)||(o.useSimmental=r),xe(r)}),W("change",function(r){De(e);let o=D();return xe(o.onVariantChange("useSimmental",r))}),v(22),p(),h(23,"mat-slide-toggle",13),Js("ngModelChange",function(r){De(e);let o=D();return _u(o.useBrahman,r)||(o.useBrahman=r),xe(r)}),W("change",function(r){De(e);let o=D();return xe(o.onVariantChange("useBrahman",r))}),v(24),p()(),h(25,"h3"),v(26),p(),h(27,"div",14)(28,"mat-slide-toggle",15),W("change",function(r){De(e);let o=D();return xe(o.onExpansionChange(r))}),v(29),p()()()(),h(30,"mat-sidenav-content")(31,"div",16)(32,"div",17)(33,"div",18)(34,"button",19),W("click",function(){De(e);let r=D();return xe(r.randomizeSetup())}),h(35,"span"),v(36),p()()(),h(37,"mat-grid-list",20),k(38,UB,9,1,"mat-grid-tile"),k(39,GB,9,1,"mat-grid-tile"),h(40,"mat-grid-tile")(41,"mat-grid-tile-header")(42,"h3")(43,"span"),v(44),p(),v(45,": "),p()(),h(46,"div",21),It(47,WB,3,2,"span",22,Ks),p()()(),h(49,"mat-card")(50,"mat-card-header")(51,"h3")(52,"span"),v(53),p()()(),h(54,"mat-card-content")(55,"ol",23)(56,"li"),v(57),h(58,"ul",24)(59,"li"),v(60),p(),h(61,"li"),v(62),p()()(),h(63,"li"),v(64),h(65,"ul",24)(66,"li"),v(67),p(),h(68,"li"),v(69),p(),h(70,"li"),v(71),p(),h(72,"li"),v(73),p()()(),h(74,"li"),v(75),h(76,"ul",24)(77,"li"),v(78),p(),h(79,"li"),v(80),p(),h(81,"li"),v(82),p()()(),h(83,"li"),k(84,qB,2,1,"ng-container"),k(85,QB,2,1,"ng-container"),k(86,YB,2,1,"ng-container"),h(87,"ul",24),k(88,XB,3,1,"li"),k(89,tj,3,1,"li"),k(90,rj,3,1,"li"),k(91,oj,2,1,"li"),k(92,aj,2,1,"li"),k(93,sj,2,1,"li"),p()(),k(94,lj,2,1,"li"),h(95,"li"),v(96),h(97,"ul",24)(98,"li"),v(99),p(),h(100,"li"),v(101),p(),k(102,cj,2,1,"li"),k(103,dj,2,1,"li"),p()(),k(104,uj,2,1,"li"),k(105,fj,2,1,"li"),k(106,mj,9,4,"li"),k(107,hj,11,5,"li"),k(108,gj,8,4,"li"),p()()()()()()(),ie(109,"app-page-footer"),p(),wt()}if(t&2){let e=n.$implicit,i=Ct(5),r=D();m(),P("is-mobile",r.isXSmall),m(),j("sidebarHandle",i)("dashboardRoute","/randomizers")("titlePrefix","second-edition."),m(),Yt("padding-top",r.isXSmall?56:0,"px"),m(),j("mode",r.isXSmall?"over":"side")("fixedInViewport",r.isXSmall)("opened",r.isXSmall?"false":"opened"),m(4),E("",e("options-label"),":"),m(3),E("",e("player-count-label"),":"),m(3),V(e("player-count-select-label")),m(),j("value",r.playerCount),m(),St(r.playerCountList),m(3),E("",e("variant-label"),":"),m(2),Xs("ngModel",r.useSimmental),j("checked",r.useSimmental),m(),E(" ",e("variant-simmental")," "),m(),Xs("ngModel",r.useBrahman),j("checked",r.useBrahman),m(),E(" ",e("variant-brahman")," "),m(2),E("",e("expansion-label"),":"),m(2),j("checked",r.useRailsToTheNorth),m(),E(" ",e("expansion-rails")," "),m(5),at(Ci(64,jB,r.isXSmall)),m(2),V(e("btn-setup-label")),m(),j("cols",r.isMax1280?1:2),m(),R(r.randomNeutralBuildings.length>0?38:-1),m(),R(r.randomStationMasters&&r.randomStationMasters.length>0?39:-1),m(5),V(e("player-buildings-label")),m(3),St(r.randomPlayerBuildings),m(6),V(e("further-setup-steps-label")),m(4),E(" ",e("further-steps-step1")," "),m(3),E(" ",e("further-steps-step1a")," "),m(2),E(" ",e("further-steps-step1b")," "),m(2),E(" ",e("further-steps-step2")," "),m(3),E(" ",e("further-steps-step21")," "),m(2),E(" ",e("further-steps-step22")," "),m(2),E(" ",e("further-steps-step23")," "),m(2),E(" ",e("further-steps-step24")," "),m(2),E(" ",e("further-steps-step3")," "),m(3),E(" ",e("further-steps-step31")," "),m(2),E(" ",e("further-steps-step32")," "),m(2),E(" ",e("further-steps-step33")," "),m(2),R(r.useSimmental&&!r.useBrahman?84:-1),m(),R(r.useBrahman&&!r.useSimmental?85:-1),m(),R(r.useBrahman||r.useSimmental?-1:86),m(2),R(r.playerCount===2?88:-1),m(),R(r.playerCount===3?89:-1),m(),R(r.playerCount===4?90:-1),m(),R(r.useSimmental?91:-1),m(),R(r.useSimmental?92:-1),m(),R(r.useBrahman?93:-1),m(),R(r.useRailsToTheNorth?-1:94),m(2),E(" ",e("further-steps-step6")," "),m(3),V(e("further-steps-step61")),m(2),V(e("further-steps-step62")),m(),R(r.playerCount>=3?102:-1),m(),R(r.playerCount>3?103:-1),m(),R(r.useRailsToTheNorth?104:-1),m(),R(r.useRailsToTheNorth?105:-1),m(),R(r.useRailsToTheNorth?106:-1),m(),R(r.useRailsToTheNorth?107:-1),m(),R(r.useRailsToTheNorth?108:-1)}}var ym=class t{dialog=d(cc);applicationConfigService=d(vm);responsive=d(Fn);storage=d(Za);randomNeutralBuildings;randomPlayerBuildings;randomStationMasters;playerCount;playerCountList;isXSmall;isMax1280;useSimmental;useBrahman;useRailsToTheNorth;ngOnInit(){this.playerCount=2,this.playerCountList=[{label:"2",value:2},{label:"3",value:3},{label:"4",value:4}],this.useSimmental=!1,this.useRailsToTheNorth=!1,this.useBrahman=!1,this.responsive.observe(ar.XSmall).subscribe(o=>{this.isXSmall=o.matches}),this.responsive.observe("(max-width: 1280px)").subscribe(o=>{this.isMax1280=o.matches});let n=this.storage.get("gwt2-playerCount");typeof n=="number"?this.emitPlayerCount(n):this.storage.set("gwt2-playerCount",2);let e=this.storage.get("gwt2-useSimmental");typeof e=="boolean"?this.applicationConfigService.useVariant.emit({name:"useSimmental",checked:e}):this.storage.set("gwt2-useSimmental",!1);let i=this.storage.get("gwt2-useBrahman");typeof i=="boolean"?this.applicationConfigService.useVariant.emit({name:"useBrahman",checked:i}):this.storage.set("gwt2-useBrahman",!1);let r=this.storage.get("gwt2-useRailsToTheNorth");typeof r=="boolean"?this.applicationConfigService.useRailsToTheNorth.emit(r):this.storage.set("gwt2-useRailsToTheNorth",!1),this.applicationConfigService.playerCount.subscribe(o=>{this.playerCount=o}),this.applicationConfigService.useVariant.subscribe(o=>{o.name==="useSimmental"?this.useSimmental=o.checked:o.name==="useBrahman"&&(this.useBrahman=o.checked)}),this.applicationConfigService.useRailsToTheNorth.subscribe(o=>{this.useRailsToTheNorth=o}),this.randomizeSetup()}openDialog(){return this.dialog.open(bm)}emitPlayerCount(n){this.applicationConfigService.playerCount.emit(n)}onPlayerCountChange(n){this.storage.set("gwt2-playerCount",n.value),this.emitPlayerCount(Number(n.value))}resetVariants(){this.openDialog().afterClosed().subscribe(()=>{this.storage.set("gwt2-useSimmental",!1),this.useSimmental=!1,this.applicationConfigService.useVariant.emit({name:"useSimmental",checked:!1}),this.storage.set("gwt2-useBrahman",!1),this.useBrahman=!1,this.applicationConfigService.useVariant.emit({name:"useBrahman",checked:!1})})}onVariantChange(n,e){this.useBrahman&&n==="useSimmental"&&e.checked||this.useSimmental&&n==="useBrahman"&&e.checked?this.resetVariants():(this.storage.set(`gwt2-${e.source.name}`,e.checked),this.applicationConfigService.useVariant.emit({name:n,checked:e.checked}))}onExpansionChange(n){this.storage.set("gwt2-useRailsToTheNorth",n.checked),this.applicationConfigService.useRailsToTheNorth.emit(n.checked)}randomizeSetup(){this.randomNeutralBuildings=this.applicationConfigService.getRandomNeutralBuildingOrder(),this.randomStationMasters=this.applicationConfigService.getRandomStationMasters(),this.randomPlayerBuildings=this.applicationConfigService.getRandomPlayerBuildings()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=I({type:t,selectors:[["app-second-edition"]],decls:1,vars:1,consts:[["sidenav",""],[4,"transloco","translocoRead"],[1,"second-edition-component","flex","flex-col"],[3,"sidebarHandle","dashboardRoute","titlePrefix"],[1,"sidenav-container","flex-1"],["fixedTopGap","56",1,"sidenav",3,"mode","fixedInViewport","opened"],[1,"sidenav__inner","p-4"],[1,"divider","mb-4"],["appearance","fill"],[3,"selectionChange","value"],[3,"value"],[1,"options-list","space-y-2","mb-4"],["color","primary","name","useSimmental",1,"options-list__option",3,"ngModelChange","change","ngModel","checked"],["color","primary","name","useBrahman",1,"options-list__option",3,"ngModelChange","change","ngModel","checked"],[1,"options-list","space-y-2"],["color","primary","name","useRailsToTheNorth",1,"options-list__option",3,"change","checked"],[1,"sidenav-content","flex","flex-col","min-h-full"],[1,"flex-1","px-2","xSmall:px-10","py-5"],[1,"mb-4"],["mat-flat-button","","color","primary",3,"click"],["rowHeight","180px","gutterSize","5px",3,"cols"],[1,"flex","flex-wrap"],[1,"px-2","text-2xl","xSmall:text-xl","flex-1"],[1,"pl-4","list-decimal"],[1,"list-disc","pl-8","text-gray-400"],[1,"flex"],[1,"px-2","text-2xl","xSmall:text-xl"],["alt","Station Master",1,"px-1","xSmall:px-2","md:px-4",3,"src"],[1,"whitespace-nowrap"]],template:function(e,i){e&1&&ze(0,_j,110,66,"ng-container",1),e&2&&j("translocoRead","second-edition")},dependencies:[iI,KE,Cv,Kt,nc,im,Ua,ic,oi,Av,$a,Ga,Wa,Ri,hr,za,ai,mm,sc,um,fm,Ka,_r,mr],styles:[`.is-mobile[_ngcontent-%COMP%]   .sidenav-container[_ngcontent-%COMP%]{flex-shrink:0;flex-grow:1;flex-basis:auto}.divider[_ngcontent-%COMP%]{margin-bottom:16px!important}.options-list[_ngcontent-%COMP%]{display:flex;flex-direction:column}
`]})};var aS=[{path:"",component:_m},{path:"argentina",component:gm},{path:"2nd-edition",component:ym},{path:"**",redirectTo:"",pathMatch:"full"}];var sS={providers:[k_(aS),i_(r_()),pE({config:{availableLangs:["de","en","pl"],defaultLang:"en",fallbackLang:"en",missingHandler:{useFallbackTranslation:!0},reRenderOnLangChange:!0,prodMode:!Ng()},loader:Af}),ld(ex.register("ngsw-worker.js",{enabled:!Ng(),registrationStrategy:"registerWhenStable:30000"}))]};Yg(Rf,sS).catch(t=>console.error(t));
