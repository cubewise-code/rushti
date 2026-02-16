import { FileJson, Folder, ChevronDown, ArrowRight } from 'lucide-react'
import { useEffect, useState } from 'react'

export default function AnimatedIDE() {
  const [isVisible, setIsVisible] = useState(false)

  useEffect(() => {
    // Trigger animation after component mounts
    const timer = setTimeout(() => setIsVisible(true), 100)
    return () => clearTimeout(timer)
  }, [])

  const taskFile = {
    name: 'daily-refresh.json',
    folder: 'workflows',
    version: '2.0',
    metadata: {
      workflow: 'daily_refresh',
      name: 'daily_refresh_optimized',
      description: 'Daily refresh workflow to update from MDM and source systems',
      author: 'cubewise-code',
      optimized: true,
      optimization_date: '2026-01-20T14:10:25.180114',
      optimization_algorithm: 'shortest_first',
      algorithm: 'EWMA',
      ewma_alpha: 0.3,
      lookback_runs: 10,
      run_count: 10,
      original_taskfile: 'workflows/daily/daily_refresh.json',
      task_count: 3
    },
    tasks: [
      { id: 'load_exchange_rates', process: 'Data.Load.ExchangeRates', deps: [] },
      { id: 'load_actuals', process: 'Data.Load.Actuals', deps: ['load_exchange_rates'] },
      { id: 'calc_variance', process: 'Calc.Variance', deps: ['load_actuals'] },
    ],
    color: 'text-sky-600',
  }

  // Animation styles for each panel
  const baseTransition = 'all 1s cubic-bezier(0.16, 1, 0.3, 1)'

  const containerStyle = {
    perspective: '2000px',
    perspectiveOrigin: '50% 40%'
  }

  const windowStyle = {
    transform: isVisible
      ? 'rotateY(10deg) rotateX(2deg) translateZ(0)'
      : 'rotateY(25deg) rotateX(8deg) translateZ(-100px) translateY(60px)',
    opacity: isVisible ? 1 : 0,
    transition: baseTransition,
    transformOrigin: 'center center',
    boxShadow: isVisible
      ? '0 25px 50px -12px rgba(0, 0, 0, 0.25), 0 12px 24px -8px rgba(0, 0, 0, 0.15)'
      : '0 10px 30px -10px rgba(0, 0, 0, 0.1)'
  }

  const leftPanelStyle = {
    opacity: isVisible ? 1 : 0,
    transform: isVisible ? 'translateX(0)' : 'translateX(-30px)',
    transition: `${baseTransition}`,
    transitionDelay: '0.2s'
  }

  const middlePanelStyle = {
    opacity: isVisible ? 1 : 0,
    transform: isVisible ? 'translateY(0)' : 'translateY(40px)',
    transition: `${baseTransition}`,
    transitionDelay: '0.35s'
  }

  const rightPanelStyle = {
    opacity: isVisible ? 1 : 0,
    transform: isVisible ? 'translateX(0)' : 'translateX(30px)',
    transition: `${baseTransition}`,
    transitionDelay: '0.5s'
  }

  const titleBarStyle = {
    opacity: isVisible ? 1 : 0,
    transform: isVisible ? 'translateY(0)' : 'translateY(-20px)',
    transition: `${baseTransition}`,
    transitionDelay: '0.1s'
  }

  return (
    <div className="relative w-full max-w-[90vw] xl:max-w-[1400px] mx-auto pt-4 pb-12" style={containerStyle}>
      {/* Light IDE Window - Tilted to the right */}
      <div
        className="bg-white rounded-xl overflow-hidden border border-slate-200"
        style={windowStyle}
      >
        {/* Title Bar */}
        <div
          className="bg-slate-50 px-4 py-2.5 flex items-center justify-between border-b border-slate-200"
          style={titleBarStyle}
        >
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 rounded-full bg-red-400" />
            <div className="w-3 h-3 rounded-full bg-yellow-400" />
            <div className="w-3 h-3 rounded-full bg-green-400" />
          </div>
          <div className="text-xs text-slate-600 font-medium">RushTI Workflow Editor</div>
          <div className="w-16" />
        </div>

        {/* Main Content Area - 3 Panel Layout */}
        <div className="flex items-stretch" style={{ backfaceVisibility: 'hidden' }}>
          {/* Left Panel - File Explorer */}
          <div
            className="w-48 bg-slate-50 border-r border-slate-200 p-3 flex-shrink-0"
            style={leftPanelStyle}
          >
            <div className="text-[10px] text-slate-400 uppercase tracking-wider mb-3 font-semibold">
              Explorer
            </div>

            {/* Expanded Folder */}
            <div className="mb-2">
              <div className="flex items-center space-x-1.5 px-2 py-1 text-slate-600">
                <ChevronDown className="w-3 h-3" />
                <Folder className="w-3.5 h-3.5 text-sky-500" />
                <span className="text-xs font-medium">workflows</span>
              </div>

              {/* Workflow files list */}
              <div className="ml-4 mt-1 space-y-0.5">
                {[
                  { name: 'daily-refresh.json', active: true },
                  { name: 'monthly-close.json', active: false },
                  { name: 'budget-load.json', active: false },
                  { name: 'fx-rates-sync.json', active: false },
                  { name: 'hr-headcount.json', active: false },
                  { name: 'consolidation.json', active: false },
                  { name: 'sales-forecast.json', active: false },
                  { name: 'inventory-refresh.json', active: false },
                  { name: 'gl-reconciliation.json', active: false },
                  { name: 'cost-allocation.json', active: false },
                  { name: 'interco-elimination.json', active: false },
                ].map((file) => (
                  <div
                    key={file.name}
                    className={`w-full flex items-center space-x-2 px-2 py-1 rounded-md ${
                      file.active
                        ? 'bg-white shadow-sm border border-slate-200'
                        : 'hover:bg-slate-100'
                    }`}
                  >
                    <FileJson className={`w-3.5 h-3.5 ${file.active ? taskFile.color : 'text-slate-400'} flex-shrink-0`} />
                    <span className={`text-xs truncate ${file.active ? 'text-slate-900 font-medium' : 'text-slate-500'}`}>
                      {file.name}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Middle Panel - JSON Editor */}
          <div
            className="flex-1 bg-white border-r border-slate-200 p-5"
            style={middlePanelStyle}
          >
            <div className="mb-3 pb-3 border-b border-slate-100">
              <div className="flex items-center space-x-2">
                <FileJson className={`w-4 h-4 ${taskFile.color}`} />
                <span className="text-sm font-semibold text-slate-900">{taskFile.name}</span>
              </div>
            </div>

            {/* JSON Content */}
            <div className="font-mono text-xs leading-relaxed">
              <div className="text-slate-600">{'{'}</div>

              {/* Version */}
              <div className="ml-3">
                <div><span className="text-sky-600">"version"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.version}"</span><span className="text-slate-600">,</span></div>
              </div>

              {/* Metadata Section */}
              <div className="ml-3 mt-1">
                <div className="text-sky-600">"metadata"</div>
                <div className="text-slate-600">: {'{'}</div>
                <div className="ml-3">
                  <div><span className="text-sky-600">"workflow"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.workflow}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"name"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.name}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"description"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.description}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"author"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.author}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600 bg-purple-100 px-1 rounded">"optimized"</span><span className="text-slate-600">: </span><span className="text-purple-600">{taskFile.metadata.optimized.toString()}</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"optimization_date"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.optimization_date}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600 bg-purple-100 px-1 rounded">"optimization_algorithm"</span><span className="text-slate-600">: </span><span className="text-purple-600">"{taskFile.metadata.optimization_algorithm}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"algorithm"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.algorithm}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"ewma_alpha"</span><span className="text-slate-600">: </span><span className="text-amber-600">{taskFile.metadata.ewma_alpha}</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"lookback_runs"</span><span className="text-slate-600">: </span><span className="text-amber-600">{taskFile.metadata.lookback_runs}</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"run_count"</span><span className="text-slate-600">: </span><span className="text-amber-600">{taskFile.metadata.run_count}</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"original_taskfile"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{taskFile.metadata.original_taskfile}"</span><span className="text-slate-600">,</span></div>
                  <div><span className="text-sky-600">"task_count"</span><span className="text-slate-600">: </span><span className="text-amber-600">{taskFile.metadata.task_count}</span></div>
                </div>
                <div className="text-slate-600">{'}'}<span>,</span></div>
              </div>

              {/* Tasks Section */}
              <div className="ml-3 mt-1">
                <div className="text-sky-600">"tasks"</div>
                <div className="text-slate-600">: [</div>
                {taskFile.tasks.map((task, idx) => (
                  <div key={task.id} className="ml-3">
                    <div className="text-slate-600">{'{'}</div>
                    <div className="ml-3">
                      <div><span className="text-sky-600">"id"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{task.id}"</span><span className="text-slate-600">,</span></div>
                      <div><span className="text-sky-600">"process"</span><span className="text-slate-600">: </span><span className="text-emerald-600">"{task.process}"</span>{task.deps.length > 0 ? <span className="text-slate-600">,</span> : ''}</div>
                      {task.deps.length > 0 && (
                        <div>
                          <span className="text-sky-600 bg-amber-100 px-1 rounded">"dependencies"</span>
                          <span className="text-slate-600">: [</span>
                          {task.deps.map((dep, i) => (
                            <span key={i}>
                              <span className="text-emerald-600">"{dep}"</span>
                              {i < task.deps.length - 1 ? <span className="text-slate-600">, </span> : ''}
                            </span>
                          ))}
                          <span className="text-slate-600">]</span>
                        </div>
                      )}
                    </div>
                    <div className="text-slate-600">{'}'}{idx < taskFile.tasks.length - 1 ? ',' : ''}</div>
                  </div>
                ))}
                <div className="text-slate-600">]</div>
              </div>

              <div className="text-slate-600">{'}'}</div>
            </div>
          </div>

          {/* Right Panel - DAG Visualization */}
          <div
            className="w-80 bg-slate-50 flex flex-col"
            style={rightPanelStyle}
          >
            {/* Header */}
            <div className="bg-white border-b border-slate-200 px-4 py-3">
              <div className="text-sm font-semibold text-slate-900">Workflow DAG</div>
              <div className="text-xs text-slate-500 mt-0.5">Execution flow</div>
            </div>

            {/* DAG Content */}
            <div className="flex-1 p-6 flex flex-col items-center justify-center">
              <div className="space-y-6 w-full">
                {/* Task 1 - Root */}
                <div className="flex flex-col items-center">
                  <div className="bg-white border-2 border-sky-500 rounded-lg p-3 shadow-sm w-full max-w-[200px]">
                    <div className="text-xs font-mono text-slate-600 mb-1">load_exchange_rates</div>
                    <div className="text-[10px] text-slate-500 truncate">Data.Load.ExchangeRates</div>
                  </div>
                  {/* Arrow down */}
                  <div className="flex flex-col items-center my-2">
                    <div className="w-0.5 h-8 bg-slate-300"></div>
                    <ArrowRight className="w-3 h-3 text-slate-400 rotate-90" />
                  </div>
                </div>

                {/* Task 2 - Depends on Task 1 */}
                <div className="flex flex-col items-center">
                  <div className="bg-white border-2 border-amber-500 rounded-lg p-3 shadow-sm w-full max-w-[200px]">
                    <div className="text-xs font-mono text-slate-600 mb-1">load_actuals</div>
                    <div className="text-[10px] text-slate-500 truncate">Data.Load.Actuals</div>
                    <div className="mt-2 pt-2 border-t border-slate-100">
                      <div className="text-[9px] text-amber-600 font-medium">Depends on:</div>
                      <div className="text-[9px] text-slate-500 font-mono">load_exchange_rates</div>
                    </div>
                  </div>
                  {/* Arrow down */}
                  <div className="flex flex-col items-center my-2">
                    <div className="w-0.5 h-8 bg-slate-300"></div>
                    <ArrowRight className="w-3 h-3 text-slate-400 rotate-90" />
                  </div>
                </div>

                {/* Task 3 - Depends on Task 2 */}
                <div className="flex flex-col items-center">
                  <div className="bg-white border-2 border-emerald-500 rounded-lg p-3 shadow-sm w-full max-w-[200px]">
                    <div className="text-xs font-mono text-slate-600 mb-1">calc_variance</div>
                    <div className="text-[10px] text-slate-500 truncate">Calc.Variance</div>
                    <div className="mt-2 pt-2 border-t border-slate-100">
                      <div className="text-[9px] text-amber-600 font-medium">Depends on:</div>
                      <div className="text-[9px] text-slate-500 font-mono">load_actuals</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
