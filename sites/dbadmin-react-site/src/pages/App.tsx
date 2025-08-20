 import React from 'react';
 import VideosTable from '../components/VideosTable';
 import { Clapperboard } from 'lucide-react';

 export default function App() {

  return (
    <div className="max-w-7xl mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold mb-6 flex items-center gap-2">
        <Clapperboard className="h-7 w-7 text-gray-700" aria-hidden />
        <span>Videos</span>
      </h1>
      <VideosTable />
    </div>
  );
 }

//commenting out test code, but leaving in place
//import TooltipTest from '../components/TooltipTest';
//function App() {
//  return (
//    <div className="p-8">
 //     <TooltipTest />
 //   </div>
 // );
//}
//export default App;
