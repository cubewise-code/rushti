import { useEffect, useState, useRef } from 'react'
import { Database, ArrowDownToLine, ArrowUpFromLine, Check, Table2 } from 'lucide-react'

// Mockup of TM1 cube view - Task Inputs (Normal Mode)
function TaskInputsView() {
  const data = [
    { id: 1, instance: 'tm1srv01', process: '}bedrock.server.wait', parameters: '{"pWaitSec": "1"}', wait: '' },
    { id: 2, instance: '', process: '', parameters: '', wait: 'true' },
    { id: 3, instance: 'tm1srv01', process: '}bedrock.server.wait', parameters: '{"pWaitSec": "2"}', wait: '' },
    { id: 4, instance: 'tm1srv01', process: '}bedrock.server.wait', parameters: '{"pWaitSec": "3"}', wait: '' },
    { id: 5, instance: 'tm1srv01', process: '}bedrock.server.wait', parameters: '{"pWaitSec": "4"}', wait: '' },
    { id: 6, instance: '', process: '', parameters: '', wait: 'true' },
  ]

  return (
    <div className="text-[11px]">
      {/* Header row */}
      <div className="grid grid-cols-5 gap-px bg-slate-200 text-slate-600 font-semibold">
        <div className="bg-slate-100 px-3 py-2">rushti_task_id</div>
        <div className="bg-slate-100 px-3 py-2">instance</div>
        <div className="bg-slate-100 px-3 py-2">process</div>
        <div className="bg-slate-100 px-3 py-2">parameters</div>
        <div className="bg-slate-100 px-3 py-2">wait</div>
      </div>
      {/* Data rows */}
      {data.map((row, i) => (
        <div key={i} className="grid grid-cols-5 gap-px bg-slate-200">
          <div className="bg-white px-3 py-1.5 text-slate-700">{row.id}</div>
          <div className="bg-white px-3 py-1.5 text-slate-600 truncate">{row.instance}</div>
          <div className="bg-white px-3 py-1.5 text-sky-600 truncate">{row.process}</div>
          <div className="bg-white px-3 py-1.5 text-emerald-600 truncate font-mono text-[10px]">{row.parameters}</div>
          <div className="bg-white px-3 py-1.5 text-amber-600">{row.wait}</div>
        </div>
      ))}
    </div>
  )
}

// Mockup of TM1 cube view - Optimized Tasks
function OptimizedTasksView() {
  const data = [
    { id: 1, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "1"}', predecessors: '', stage: 'load', timeout: '', cancel: '' },
    { id: 2, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "2"}', predecessors: '', stage: 'load', timeout: '', cancel: '' },
    { id: 3, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "5"}', predecessors: '2', stage: 'transfer', timeout: '10', cancel: '' },
    { id: 4, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "1"}', predecessors: '2', stage: 'transfer', timeout: '10', cancel: 'true' },
    { id: 5, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "1"}', predecessors: '3', stage: 'calc', timeout: '', cancel: '' },
    { id: 6, process: '}bedrock.server.wait', parameters: '{"pWaitSec": "1"}', predecessors: '1,5', stage: 'export', timeout: '', cancel: '' },
  ]

  const stageColors = {
    load: 'bg-sky-100 text-sky-700',
    transfer: 'bg-amber-100 text-amber-700',
    calc: 'bg-violet-100 text-violet-700',
    export: 'bg-emerald-100 text-emerald-700',
  }

  return (
    <div className="text-[11px]">
      {/* Header row */}
      <div className="grid grid-cols-7 gap-px bg-slate-200 text-slate-600 font-semibold">
        <div className="bg-slate-100 px-3 py-2">task_id</div>
        <div className="bg-slate-100 px-3 py-2">process</div>
        <div className="bg-slate-100 px-3 py-2">parameters</div>
        <div className="bg-slate-100 px-3 py-2">predecessors</div>
        <div className="bg-slate-100 px-3 py-2">stage</div>
        <div className="bg-slate-100 px-3 py-2">timeout</div>
        <div className="bg-slate-100 px-3 py-2">cancel_at_timeout</div>
      </div>
      {/* Data rows */}
      {data.map((row, i) => (
        <div key={i} className="grid grid-cols-7 gap-px bg-slate-200">
          <div className="bg-white px-3 py-1.5 text-slate-700">{row.id}</div>
          <div className="bg-white px-3 py-1.5 text-sky-600 truncate">{row.process}</div>
          <div className="bg-white px-3 py-1.5 text-emerald-600 truncate font-mono text-[10px]">{row.parameters}</div>
          <div className="bg-white px-3 py-1.5 text-amber-600 font-mono">{row.predecessors}</div>
          <div className="bg-white px-3 py-1.5">
            <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${stageColors[row.stage]}`}>
              {row.stage}
            </span>
          </div>
          <div className="bg-white px-3 py-1.5 text-slate-600">{row.timeout}</div>
          <div className="bg-white px-3 py-1.5 text-slate-600">{row.cancel}</div>
        </div>
      ))}
    </div>
  )
}

// Mockup of TM1 cube view - Execution Results
function ExecutionResultsView() {
  const data = [
    { id: 1, process: '}bedrock.server.wait', status: 'Success', start: '11:36:01', end: '11:36:03', duration: '1.076', predecessors: '' },
    { id: 2, process: '}bedrock.server.wait', status: 'Success', start: '11:36:03', end: '11:36:05', duration: '2.38', predecessors: '["1"]' },
    { id: 3, process: '}bedrock.server.wait', status: 'Success', start: '11:36:03', end: '11:36:06', duration: '3.399', predecessors: '["1"]' },
    { id: 4, process: '}bedrock.server.wait', status: 'Success', start: '11:36:03', end: '11:36:07', duration: '4.436', predecessors: '["1"]' },
    { id: 5, process: '}bedrock.server.wait', status: 'Success', start: '11:36:07', end: '11:36:12', duration: '5.21', predecessors: '["2","3"]' },
    { id: 6, process: '}bedrock.server.wait', status: 'Success', start: '11:36:12', end: '11:36:14', duration: '1.89', predecessors: '["5"]' },
  ]

  return (
    <div className="text-[11px]">
      {/* Header row */}
      <div className="grid grid-cols-7 gap-px bg-slate-200 text-slate-600 font-semibold">
        <div className="bg-slate-100 px-3 py-2">task_id</div>
        <div className="bg-slate-100 px-3 py-2">process</div>
        <div className="bg-slate-100 px-3 py-2">status</div>
        <div className="bg-slate-100 px-3 py-2">start_time</div>
        <div className="bg-slate-100 px-3 py-2">end_time</div>
        <div className="bg-slate-100 px-3 py-2">duration_s</div>
        <div className="bg-slate-100 px-3 py-2">predecessors</div>
      </div>
      {/* Data rows */}
      {data.map((row, i) => (
        <div key={i} className="grid grid-cols-7 gap-px bg-slate-200">
          <div className="bg-white px-3 py-1.5 text-slate-700">{row.id}</div>
          <div className="bg-white px-3 py-1.5 text-sky-600 truncate">{row.process}</div>
          <div className="bg-white px-3 py-1.5">
            <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-emerald-100 text-emerald-700">
              {row.status}
            </span>
          </div>
          <div className="bg-white px-3 py-1.5 text-slate-500 font-mono">{row.start}</div>
          <div className="bg-white px-3 py-1.5 text-slate-500 font-mono">{row.end}</div>
          <div className="bg-white px-3 py-1.5 text-amber-600 font-mono">{row.duration}</div>
          <div className="bg-white px-3 py-1.5 text-slate-400 font-mono text-[9px] truncate">{row.predecessors}</div>
        </div>
      ))}
    </div>
  )
}

export default function PAIntegration() {
  const [isVisible, setIsVisible] = useState(false)
  const [activeIndex, setActiveIndex] = useState(0)
  const sectionRef = useRef(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true)
        }
      },
      { threshold: 0.2 }
    )

    if (sectionRef.current) {
      observer.observe(sectionRef.current)
    }

    return () => observer.disconnect()
  }, [])

  // Auto-cycle through views every 3 seconds
  useEffect(() => {
    if (!isVisible) return

    const interval = setInterval(() => {
      setActiveIndex((prev) => (prev + 1) % 3)
    }, 3000)

    return () => clearInterval(interval)
  }, [isVisible])

  const views = [
    {
      id: 'inputs',
      title: 'Task Inputs',
      subtitle: 'rushti_inputs_norm',
      description: 'Define task configurations directly in TM1 cubes',
      component: TaskInputsView,
    },
    {
      id: 'optimal',
      title: 'Optimized Tasks',
      subtitle: 'rushti_inputs_opt',
      description: 'Advanced scheduling with stages and dependencies',
      component: OptimizedTasksView,
    },
    {
      id: 'results',
      title: 'Execution Results',
      subtitle: 'rushti_results',
      description: 'Track execution status, timing, and errors',
      component: ExecutionResultsView,
    },
  ]

  const features = [
    {
      icon: ArrowDownToLine,
      title: 'Read from Cube',
      description: 'Load task definitions directly from TM1 cubes, no JSON files needed',
      color: 'sky',
    },
    {
      icon: ArrowUpFromLine,
      title: 'Write Results Back',
      description: 'Execution results, timing, and errors automatically written to TM1',
      color: 'emerald',
    },
    {
      icon: Database,
      title: 'Native Integration',
      description: 'Seamless connection with IBM Planning Analytics REST API',
      color: 'amber',
    },
  ]

  return (
    <section ref={sectionRef} className="py-24 relative bg-white overflow-hidden">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16">
          <h2
            className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-4 transition-all duration-700 ${
              isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'
            }`}
          >
            Native Planning Analytics Integration
          </h2>
          <p
            className={`text-slate-600 text-lg max-w-2xl mx-auto transition-all duration-700 delay-100 ${
              isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'
            }`}
          >
            Manage your workflows entirely within TM1. Read task definitions from cubes and write execution results back, keeping everything in one place.
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-5 gap-12 items-start">
          {/* Left side - Stacked Cubewise Arc views with 3D tilt - takes 3 columns */}
          <div
            className={`relative lg:col-span-3 h-[520px] transition-all duration-1000 delay-200 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-12'
            }`}
          >
            {/* View stack with perspective - cards cascade down-right */}
            <div
              className="relative w-full h-full"
              style={{
                perspective: '2000px',
                perspectiveOrigin: '30% 40%'
              }}
            >
              {views.map((view, index) => {
                // Calculate position based on active index
                const position = (index - activeIndex + 3) % 3

                // Stacked positions - front at top-left, cascade down to the right
                // NO SCALE changes - all cards maintain same size
                const transforms = {
                  0: { // Front/active - top left position
                    translateX: 0,
                    translateY: 0,
                    translateZ: 0,
                    rotateY: 8,
                    rotateX: 3,
                    opacity: 1,
                    zIndex: 30,
                  },
                  1: { // Middle - offset down-right
                    translateX: 40,
                    translateY: 40,
                    translateZ: -40,
                    rotateY: 8,
                    rotateX: 3,
                    opacity: 0.85,
                    zIndex: 20,
                  },
                  2: { // Back - further down-right
                    translateX: 80,
                    translateY: 80,
                    translateZ: -80,
                    rotateY: 8,
                    rotateX: 3,
                    opacity: 0.7,
                    zIndex: 10,
                  },
                }

                const t = transforms[position]
                const ViewComponent = view.component

                return (
                  <div
                    key={view.id}
                    className="absolute top-0 left-0 w-[92%] cursor-pointer transition-all duration-700 ease-out"
                    style={{
                      transform: `
                        translateX(${t.translateX}px)
                        translateY(${t.translateY}px)
                        translateZ(${t.translateZ}px)
                        rotateY(${t.rotateY}deg)
                        rotateX(${t.rotateX}deg)
                      `,
                      opacity: t.opacity,
                      zIndex: t.zIndex,
                      transformStyle: 'preserve-3d',
                    }}
                    onClick={() => setActiveIndex(index)}
                  >
                    <div
                      className="bg-white rounded-xl overflow-hidden flex flex-col"
                      style={{
                        boxShadow: position === 0
                          ? '0 25px 50px -12px rgba(0, 0, 0, 0.25), 0 12px 24px -8px rgba(0, 0, 0, 0.15)'
                          : '0 15px 30px -10px rgba(0, 0, 0, 0.15), 0 8px 16px -6px rgba(0, 0, 0, 0.1)',
                        border: '1px solid rgba(226, 232, 240, 0.8)',
                      }}
                    >
                      {/* Window header - Cubewise Arc style */}
                      <div className="bg-slate-50 px-4 py-2.5 flex items-center justify-between border-b border-slate-200">
                        <div className="flex items-center space-x-2">
                          <div className="w-3 h-3 rounded-full bg-red-400" />
                          <div className="w-3 h-3 rounded-full bg-amber-400" />
                          <div className="w-3 h-3 rounded-full bg-emerald-400" />
                        </div>
                        <div className="flex items-center gap-2">
                          <Table2 className="w-4 h-4 text-slate-400" />
                          <span className="text-sm text-slate-600 font-medium">
                            Cubewise Arc - rushti cube
                          </span>
                        </div>
                        <div className="w-20" />
                      </div>

                      {/* Toolbar - mimicking Arc view selector */}
                      <div className="bg-white border-b border-slate-200 px-4 py-2.5 flex items-center gap-3">
                        <div className="flex items-center gap-1.5 px-3 py-1.5 bg-sky-50 border border-sky-200 rounded text-xs text-slate-700">
                          <span className="text-sky-600 font-semibold">rushti_measure:</span>
                          <span className="font-mono text-slate-600">{view.subtitle}</span>
                        </div>
                      </div>

                      {/* Data table content - larger */}
                      <div className="overflow-hidden p-4 bg-slate-50">
                        <ViewComponent />
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>

            {/* View indicators */}
            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 flex items-center gap-2">
              {views.map((view, index) => (
                <button
                  key={view.id}
                  onClick={() => setActiveIndex(index)}
                  className={`transition-all duration-300 ${
                    index === activeIndex
                      ? 'w-8 h-2 bg-sky-500 rounded-full'
                      : 'w-2 h-2 bg-slate-300 rounded-full hover:bg-slate-400'
                  }`}
                  aria-label={`View ${view.title}`}
                />
              ))}
            </div>

            {/* Active view label */}
            <div
              className="absolute -bottom-6 left-1/2 -translate-x-1/2 text-center transition-all duration-300"
              key={activeIndex}
            >
              <div className="text-sm font-semibold text-slate-900">
                {views[activeIndex].title}
              </div>
              <div className="text-xs text-slate-500">
                {views[activeIndex].description}
              </div>
            </div>
          </div>

          {/* Right side - Features - takes 2 columns */}
          <div
            className={`lg:col-span-2 space-y-5 transition-all duration-1000 delay-400 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-12'
            }`}
          >
            {features.map((feature, index) => {
              const colorClasses = {
                sky: 'bg-sky-100 text-sky-600',
                emerald: 'bg-emerald-100 text-emerald-600',
                amber: 'bg-amber-100 text-amber-600',
              }

              return (
                <div
                  key={feature.title}
                  className={`flex items-start gap-4 p-6 bg-slate-50 rounded-xl border border-slate-100 transition-all duration-500 hover:shadow-lg hover:border-slate-200`}
                  style={{ transitionDelay: `${(index + 4) * 100}ms` }}
                >
                  <div className={`w-12 h-12 rounded-xl ${colorClasses[feature.color]} flex items-center justify-center flex-shrink-0`}>
                    <feature.icon className="w-6 h-6" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900 mb-1">
                      {feature.title}
                    </h3>
                    <p className="text-slate-600">
                      {feature.description}
                    </p>
                  </div>
                </div>
              )
            })}

            {/* Additional benefits */}
            <div className="mt-8 p-6 bg-gradient-to-r from-sky-50 to-cyan-50 rounded-xl border border-sky-100">
              <h4 className="font-semibold text-slate-900 mb-3">Key Benefits</h4>
              <ul className="space-y-2">
                {[
                  'Single source of truth for workflow configuration',
                  'Real-time visibility into execution status',
                  'Historical tracking for analysis and optimization',
                  'No file management, everything lives in TM1',
                ].map((benefit, i) => (
                  <li key={i} className="flex items-center gap-2 text-sm text-slate-600">
                    <Check className="w-4 h-4 text-sky-500 flex-shrink-0" />
                    {benefit}
                  </li>
                ))}
              </ul>
            </div>

            {/* Code snippet - CLI example */}
            <div className="mt-4 p-4 bg-slate-900 rounded-xl">
              <div className="text-xs text-slate-400 mb-2 font-mono">Command line</div>
              <pre className="text-sm font-mono">
                <code>
                  <span className="text-emerald-400">rushti</span>
                  <span className="text-slate-300"> build </span>
                  <span className="text-sky-400">--tm1-instance</span>
                  <span className="text-amber-300"> tm1srv01</span>
                </code>
              </pre>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
