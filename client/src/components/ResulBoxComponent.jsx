import placeholder from '@/assets/placeholder.png'
import '../App.css';

const ResulBoxComponent = () => {
  return (
    <>
    <div className="cuadro">
        <h1 className='tituloimg'>Series</h1>
        <img src={placeholder} className="cuadroimg" alt="..." />
        <div className="cuadrobody">
        </div>
        <a href="series" className="cuadrotitle">Popular Series</a>
      </div>
      <div className="cuadro">
      <h1 className='tituloimg'>Movies</h1>
        <img src={placeholder} className="cuadroimg" alt="..." />
        <div className="cuadrobody">
        </div>
        <a href="peliculas" className="cuadrotitle">Popular Movies</a>
     </div>
    </>
  )
}

export default ResulBoxComponent