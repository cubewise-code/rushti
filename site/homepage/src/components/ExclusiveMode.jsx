import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { Lock, Users, Shield, Clock, CheckCircle2, Cpu } from 'lucide-react'
import { useState, useEffect } from 'react'

// Animated visualization showing exclusive mode at workflow level
function ExclusiveModeAnimation({ isVisible }) {
  const [step, setStep] = useState(0)

  useEffect(() => {
    if (!isVisible) return

    const interval = setInterval(() => {
      setStep(prev => (prev + 1) % 5)
    }, 2500)

    return () => clearInterval(interval)
  }, [isVisible])

  // Workflows (not individual processes)
  const workflows = [
    {
      id: 'production-etl',
      label: 'Production ETL',
      exclusive: true,
      status: step >= 1 && step < 4 ? 'running' : step >= 4 ? 'completed' : 'waiting'
    },
    {
      id: 'daily-reports',
      label: 'Daily Reports',
      exclusive: false,
      status: step >= 1 && step < 4 ? 'queued' : step >= 4 ? 'running' : 'idle'
    },
    {
      id: 'ad-hoc-query',
      label: 'Ad-hoc Query',
      exclusive: false,
      status: step >= 1 && step < 4 ? 'queued' : step >= 4 ? 'running' : 'idle'
    },
  ]

  return (
    <div className="bg-white border border-slate-200 rounded-xl overflow-hidden shadow-xl">
      {/* Window header */}
      <div className="flex items-center justify-between px-4 py-3 bg-slate-50 border-b border-slate-200">
        <div className="flex items-center space-x-2">
          <div className="w-3 h-3 rounded-full bg-red-400" />
          <div className="w-3 h-3 rounded-full bg-amber-400" />
          <div className="w-3 h-3 rounded-full bg-emerald-400" />
        </div>
        <div className="flex items-center space-x-2">
          <Lock className="w-4 h-4 text-violet-500" />
          <span className="text-slate-500 text-sm font-medium">TM1 Environment</span>
        </div>
        <div className="w-16" />
      </div>

      {/* Content */}
      <div className="p-6">
        {/* Status banner */}
        <div className={`flex items-center gap-3 p-3 rounded-lg mb-6 transition-all duration-500 ${
          step >= 1 && step < 4
            ? 'bg-violet-50 border border-violet-200'
            : 'bg-slate-50 border border-slate-200'
        }`}>
          {step >= 1 && step < 4 ? (
            <>
              <Lock className="w-5 h-5 text-violet-600" />
              <div>
                <div className="text-sm font-medium text-violet-700">Exclusive Lock Active</div>
                <div className="text-xs text-violet-600">production-etl workflow has exclusive access</div>
              </div>
            </>
          ) : step >= 4 ? (
            <>
              <Users className="w-5 h-5 text-emerald-600" />
              <div>
                <div className="text-sm font-medium text-emerald-700">Lock Released</div>
                <div className="text-xs text-emerald-600">Other workflows can now proceed</div>
              </div>
            </>
          ) : (
            <>
              <Clock className="w-5 h-5 text-slate-500" />
              <div>
                <div className="text-sm font-medium text-slate-700">Checking Sessions</div>
                <div className="text-xs text-slate-500">Scanning for active RushTI workflows...</div>
              </div>
            </>
          )}
        </div>

        {/* Workflow list */}
        <div className="space-y-3">
          <div className="text-xs text-slate-400 uppercase tracking-wider font-medium mb-2">RushTI Workflows</div>
          {workflows.map((wf) => (
            <div
              key={wf.id}
              className={`flex items-center justify-between p-3 rounded-lg border transition-all duration-300 ${
                wf.status === 'running'
                  ? wf.exclusive
                    ? 'bg-violet-50 border-violet-200'
                    : 'bg-emerald-50 border-emerald-200'
                  : wf.status === 'queued'
                    ? 'bg-amber-50 border-amber-200'
                    : wf.status === 'completed'
                      ? 'bg-slate-50 border-slate-200'
                      : 'bg-slate-50 border-slate-200'
              }`}
            >
              <div className="flex items-center gap-3">
                {wf.exclusive && wf.status !== 'completed' && (
                  <Lock className="w-4 h-4 text-violet-500" />
                )}
                <div>
                  <span className="text-sm font-medium text-slate-700">{wf.label}</span>
                  <div className="text-[10px] text-slate-400 font-mono">{wf.id}.json</div>
                </div>
                {wf.exclusive && (
                  <span className="text-[10px] uppercase tracking-wider bg-violet-100 text-violet-600 px-2 py-0.5 rounded-full font-medium">
                    --exclusive
                  </span>
                )}
              </div>
              <div className="flex items-center gap-2">
                {wf.status === 'running' && (
                  <>
                    <div className={`w-2 h-2 rounded-full animate-pulse ${wf.exclusive ? 'bg-violet-500' : 'bg-emerald-500'}`} />
                    <span className={`text-xs font-medium ${wf.exclusive ? 'text-violet-600' : 'text-emerald-600'}`}>Running</span>
                  </>
                )}
                {wf.status === 'queued' && (
                  <>
                    <div className="w-2 h-2 bg-amber-500 rounded-full" />
                    <span className="text-xs text-amber-600 font-medium">Waiting</span>
                  </>
                )}
                {wf.status === 'completed' && (
                  <>
                    <CheckCircle2 className="w-4 h-4 text-slate-400" />
                    <span className="text-xs text-slate-400">Done</span>
                  </>
                )}
                {wf.status === 'idle' && (
                  <>
                    <div className="w-2 h-2 bg-slate-300 rounded-full" />
                    <span className="text-xs text-slate-400">Pending</span>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>

        {/* Console output */}
        <div className="mt-4 p-3 bg-slate-900 rounded-lg font-mono text-xs">
          <div className="text-slate-500"># rushti run --tasks production-etl.json --exclusive</div>
          {step >= 1 && (
            <div className="text-emerald-400 mt-1">Exclusive access granted, proceeding...</div>
          )}
          {step >= 4 && (
            <div className="text-slate-400 mt-1">Workflow completed, lock released</div>
          )}
        </div>
      </div>
    </div>
  )
}

export default function ExclusiveMode() {
  const [ref, isVisible] = useScrollAnimation()

  const benefits = [
    {
      icon: Shield,
      title: 'Prevent Conflicts',
      description: 'Avoid resource contention, data corruption, and process failures when multiple workflows share the same TM1 servers.'
    },
    {
      icon: Cpu,
      title: 'Prevent Server Overload',
      description: 'If you only have 60 cores and kick off 3 RushTI instances with 30 workers each, you leave no room for the server. Exclusive mode prevents this.'
    },
    {
      icon: Clock,
      title: 'Automatic Queuing',
      description: 'RushTI detects active sessions, waits for them to complete, and proceeds only when exclusive access is available.'
    },
  ]

  return (
    <section className="py-32 relative overflow-hidden bg-white">
      {/* Background gradient orbs */}
      <div className="absolute w-[800px] h-[800px] rounded-full bg-violet-400/5 blur-3xl -left-40 top-1/2 -translate-y-1/2" />
      <div className="absolute w-[600px] h-[600px] rounded-full bg-purple-400/5 blur-3xl -right-20 bottom-0" />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center" ref={ref}>
          {/* Left side - Animated visualization */}
          <div
            className={`relative animate-on-scroll ${isVisible ? 'visible' : ''}`}
            style={{
              transitionDelay: '200ms',
              perspective: '1000px'
            }}
          >
            <div
              className="relative"
              style={{
                transform: 'rotateY(8deg) rotateX(2deg)',
                transformStyle: 'preserve-3d'
              }}
            >
              {/* Glow effect */}
              <div className="absolute -inset-4 bg-gradient-to-r from-violet-400/10 to-purple-400/10 rounded-2xl blur-2xl" />

              <ExclusiveModeAnimation isVisible={isVisible} />

              {/* Floating badge */}
              <div
                className="absolute -bottom-4 -right-4 bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-lg"
                style={{ transform: 'translateZ(40px)' }}
              >
                <div className="flex items-center space-x-2">
                  <Lock className="w-4 h-4 text-violet-500" />
                  <span className="text-xs text-slate-600">Works across all TM1 instances</span>
                </div>
              </div>
            </div>
          </div>

          {/* Right side - Text content */}
          <div className={`animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            <div className="inline-flex items-center space-x-2 bg-violet-50 border border-violet-200 rounded-full px-4 py-2 mb-6">
              <Lock className="w-4 h-4 text-violet-600" />
              <span className="text-violet-700 text-sm font-medium">Exclusive Mode</span>
            </div>

            <h2 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
              One workflow
              <br />
              <span className="bg-gradient-to-r from-violet-600 to-purple-500 bg-clip-text text-transparent">at a time</span>
            </h2>

            <p className="text-slate-600 text-lg mb-8 leading-relaxed max-w-lg">
              In shared TM1 environments, multiple admins and automated systems can trigger RushTI executions simultaneously.
              Exclusive mode ensures only one workflow runs at a time, preventing conflicts and data corruption.
            </p>

            {/* Benefits */}
            <div className="space-y-4">
              {benefits.map((benefit) => (
                <div
                  key={benefit.title}
                  className="flex items-start gap-4 p-4 bg-slate-50 rounded-xl border border-slate-100"
                >
                  <div className="w-10 h-10 rounded-lg bg-violet-100 flex items-center justify-center flex-shrink-0">
                    <benefit.icon className="w-5 h-5 text-violet-600" />
                  </div>
                  <div>
                    <div className="font-semibold text-slate-900 mb-1">{benefit.title}</div>
                    <div className="text-sm text-slate-600">{benefit.description}</div>
                  </div>
                </div>
              ))}
            </div>

            {/* Code snippet - CLI example */}
            <div className="mt-8 p-4 bg-slate-900 rounded-xl">
              <div className="text-xs text-slate-400 mb-2 font-mono">Command line</div>
              <pre className="text-sm font-mono">
                <code>
                  <span className="text-emerald-400">rushti</span>
                  <span className="text-slate-300"> run </span>
                  <span className="text-sky-400">--tasks</span>
                  <span className="text-amber-300"> production-etl.json </span>
                  <span className="text-violet-400">--exclusive</span>
                </code>
              </pre>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
